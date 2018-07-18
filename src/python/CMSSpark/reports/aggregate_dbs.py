# System modules
import os

# Pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info_save
from CMSSpark.conf import OptionParser

DBS_TIME_DATA_FILE = 'spark_exec_time_tier_dbs.txt'

def get_options():
    opts = OptionParser('DBS')

    opts.parser.add_argument("--inst", action="store",
        dest="inst", default="global")

    return opts.parser.parse_args()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_destination_dir():
    return '%s/../../../bash/report_tiers' % get_script_dir()

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def run(fout, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)

    quiet_logs(ctx)
    
    sqlContext = HiveContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    ddf = tables['ddf']
    bdf = tables['bdf']
    fdf = tables['fdf']

    # join tables
    cols = ['d_dataset','d_dataset_id', 'b_block_id','b_file_count','f_block_id','f_file_id','f_dataset_id','f_event_count','f_file_size']

    # join tables
    stmt = 'SELECT %s FROM ddf JOIN bdf on ddf.d_dataset_id = bdf.b_dataset_id JOIN fdf on bdf.b_block_id=fdf.f_block_id' % ','.join(cols)
    print(stmt)
    joins = sqlContext.sql(stmt)

    # keep table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # construct aggregation
    fjoin = joins\
            .groupBy(['d_dataset'])\
            .agg({'b_file_count':'sum', 'f_event_count':'sum', 'f_file_size':'sum'})\
            .withColumnRenamed('d_dataset', 'dataset')\
            .withColumnRenamed('sum(b_file_count)', 'nfiles')\
            .withColumnRenamed('sum(f_event_count)', 'nevents')\
            .withColumnRenamed('sum(f_file_size)', 'size')

    # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        fjoin.write.format("com.databricks.spark.csv")\
                .option("header", "true").save(fout)

    ctx.stop()

@info_save('%s/%s' % (get_destination_dir(), DBS_TIME_DATA_FILE))
def main():
    "Main function"
    opts = get_options()
    print("Input arguments: %s" % opts)
    
    fout = opts.fout
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    
    run(fout, yarn, verbose, inst)

if __name__ == '__main__':
    main()
