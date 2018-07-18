#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse DBS and CMSSW records on HDFS.
"""

# system modules
import os
import re
import sys
import gzip
import time
import json

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, cmssw_tables, split_dataset
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser

def cmssw_date(date):
    "Convert given date into CMSSW date format"
    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)

def cmssw_date_unix(date):
    "Convert CMSSW date into UNIX timestamp"
    return time.mktime(time.strptime(date, 'year=%Y/month=%m/day=%d'))

def run(date, fout, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    ddf = tables['ddf'] # dataset table
    fdf = tables['fdf'] # file table

    # read CMSSW avro rdd
    date = cmssw_date(date)
    cmssw_df = cmssw_tables(ctx, sqlContext, date=date, verbose=verbose)

    # merge DBS and CMSSW data
    cols = ['d_dataset','d_dataset_id','f_logical_file_name','FILE_LFN','SITE_NAME']
    stmt = 'SELECT %s FROM ddf JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id JOIN cmssw_df ON fdf.f_logical_file_name = cmssw_df.FILE_LFN' % ','.join(cols)
    joins = sqlContext.sql(stmt)
    print_rows(joins, stmt, verbose)

    # perform aggregation
    fjoin = joins.groupBy(['SITE_NAME','d_dataset'])\
            .agg({'FILE_LFN':'count'})\
            .withColumnRenamed('count(FILE_LFN)', 'count')\
            .withColumnRenamed('d_dataset', 'dataset')\
            .withColumn('date', lit(cmssw_date_unix(date)))\
            .withColumn('count_type', lit('cmssw'))\

    # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        ndf = split_dataset(fjoin, 'dataset')
        ndf.write.format("com.databricks.spark.csv")\
                .option("header", "true").save(fout)

    ctx.stop()

@info
def main():
    "Main function"
    optmgr  = OptionParser('dbs_cmssw')
    msg = 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
    optmgr.parser.add_argument("--inst", action="store",
        dest="inst", default="global", help=msg)
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    inst = opts.inst
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    run(opts.date, opts.fout, opts.yarn, opts.verbose, inst)

if __name__ == '__main__':
    main()
