export PYTHONPATH=$PYTHONPATH:$PWD/src/python
export PATH=$PWD/bin:$PATH

cd CMSSW_9_2_0/src
eval `scramv1 runtime -sh`
cd -
