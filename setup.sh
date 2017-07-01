export PYTHONPATH=$PYTHONPATH:$PWD/src/python
export PATH=$PWD/bin:$PATH

cd /afs/cern.ch/work/q/qnguyen/public/PulseShape/CMSSW_9_0_0/src
eval `scramv1 runtime -sh`
cd -
