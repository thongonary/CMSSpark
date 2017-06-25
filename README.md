# CMSSpark

Set of spark scripts to parse and extract useful aggregated info from various
CMS data streams on HDFS. So far, it supports DBS, PhEDEx, AAA, EOS, CMSSW
data parsing on HDFS.

Here are few examples how to get various stats:

```
# First set proper paths (or run setup.sh instead) 
export PYTHONPATH=$PYTHONPATH:$PWD/src/python
export PATH=$PWD/bin:$PATH

# DBS+PhEDEx
apatterns="*BUNNIES*,*Commissioning*,*RelVal*"
hdir=hdfs:///cms/users/qnguyen/datasets
run_spark dbs_phedex.py --fout=$hdir --antipatterns=$apatterns --yarn --verbose

# Send data to CERN MONIT, user must provide stomppy egg and AMQ JSON broker file
run_spark cern_monit.py --hdir=$hdir --stomp=static/stomp.py-4.1.15-py2.7.egg --amq=amq_broker.json

# DBS+CMSSW
run_spark dbs_cmssw.py --verbose --yarn --fout=hdfs:///cms/users/qnguyen/cmssw --date=20170411

# DBS+AAA
run_spark dbs_aaa.py --verbose --yarn --fout=hdfs:///cms/users/qnguyen/aaa --date=20170411

# DBS+EOS
run_spark dbs_eos.py --verbose --yarn --fout=hdfs:///cms/users/qnguyen/eos --date=20170411

# WMArchive examples:
run_spark wmarchive.py --fout=hdfs:///cms/users/qnguyen/wma --date=20170411
run_spark wmarchive.py --fout=hdfs:///cms/users/qnguyen/wma --date=20170411,20170420 --yarn
run_spark wmarchive.py --fout=hdfs:///cms/users/qnguyen/wma --date=20170411-20170420 --yarn

# To list the output files in HDFS:
hadoop fs -ls /cms/users/qnguyen/

# Copy to local:
hadoop fs -get /cms/users/qnguyen/wma .

```

*Please note*: in order to run cern_monit.py script user must supply two
additional parameters. The StompAMQ library file and AMQ credentials.
The former is located in static are of this package. The later contains
CERN MONIT end-point parameters and should be individually obtained from CERN
MONIT team. For example

```
run_spark cern_monit.py --hdir=/cms/users/qnguyen/datasets --amq=amq_broker.json --stomp=/path/stomp.py-4.1.15-py2.7.egg
```

### CMS metadata
CMS metadata are stored in the following location on HDFS and accessing from
analytix cluster:

- DBS: /project/awg/cms/CMS_DBS3_PROD_GLOBAL/ (full DB dump in CSV data-format)
- CMSSW: /project/awg/cms/cmssw-popularity (daily snapshots in avro data-format)
- JobMonitoring: /project/awg/cms/jm-data-popularity (daily snapshots in avro data-format)
- JobMonitoring: /project/awg/cms/job-monitoring (daily snapshots in avro data-format)
- PhedexReplicas: /project/awg/cms/phedex/block-replicas-snapshots (daily snapshots in CSV data-format)
- PhedexCatalog: /project/awg/cms/phedex/catalog (daily snapshots in CSV data-format)
- AAA: /project/monitoring/archive/xrootd (daily snapshots in JSON data-format)
- EOS: /project/monitoring/archive/eos (daily snapshots in JSON data-format)
- WMArchive: /cms/wmarchive/avro (daily snapshots in Avro data-format)
