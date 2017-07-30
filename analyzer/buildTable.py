import json
import numpy as np
import pandas as pd
import h5py
from collections import defaultdict
from glob import glob
from ast import literal_eval as leval 
import math
import re 
import os, sys
import matplotlib.pyplot as plt

FullExitCode = [0, 1, 2, 4, 5, 6, 7, 8, 9, 11, 15, 24, 25, 30, 40, 50, 64, 65, 66, 73, 81, 84, 85, 90, 92, 126, 127, 129, 132, 133, 134, 135, 137, 139, 143, 147, 151, 152, 153, 155, 195, 243, 7000, 7001, 7002, 8001, 8002, 8003, 8004, 8005, 8006, 8007, 8008, 8009, 8010, 8011, 8012, 8013, 8014, 8015, 8016, 8017, 8018, 8019, 8020, 8021, 8022, 8023, 8024, 8025, 8026, 8027, 8028, 8030, 8031, 8032, 9000, 10031, 10032, 10034, 10040, 10042, 10043, 50110, 50111, 50113, 50115, 50116, 50513, 50660, 50661, 50662, 50663, 50664, 50665, 50669, 60302, 60307, 60311, 60312, 60315, 60316, 60317, 60318, 60319, 60320, 60321, 60322, 60323, 60324, 60401, 60402, 60403, 60404, 60405, 60407, 60408, 60409, 60450, 60451, 71101, 71102, 71103, 71104, 71300, 71301, 71302, 71303, 71304, 70318, 70452, 80000, 80001, 80453, 90000, 99109, 99999]
print "Number of exit codes: %d" % len(FullExitCode)

FullSite = ['T0_CH_CERN','T1_US_FNAL_Disk','T1_DE_KIT','T1_ES_PIC','T1_FR_CCIN2P3','T1_IT_CNAF','T1_RU_JINR','T1_UK_RAL','T1_US_FNAL','T2_AT_Vienna', 'T2_BE_IIHE', 'T2_BE_UCL', 'T2_BR_SPRACE', 'T2_BR_UERJ', 'T2_CH_CERN', 'T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CSCS', 'T2_CH_CSCS_HPC', 'T2_CN_Beijing', 'T2_DE_DESY', 'T2_DE_RWTH', 'T2_EE_Estonia', 'T2_ES_CIEMAT', 'T2_ES_IFCA', 'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR', 'T2_FR_IPHC', 'T2_GR_Ioannina', 'T2_HU_Budapest', 'T2_IN_TIFR', 'T2_IT_Bari', 'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome', 'T2_KR_KISTI', 'T2_KR_KNU', 'T2_MY_UPM_BIRUNI', 'T2_PK_NCP', 'T2_PL_Swierk', 'T2_PL_Warsaw', 'T2_PT_NCG_Lisbon', 'T2_RU_IHEP', 'T2_RU_INR', 'T2_RU_ITEP', 'T2_RU_JINR', 'T2_RU_PNPI', 'T2_RU_SINP', 'T2_TH_CUNSTDA', 'T2_TR_METU', 'T2_TW_NCHC', 'T2_UA_KIPT', 'T2_UK_London_Brunel', 'T2_UK_London_IC', 'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue', 'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin', 'T3_IN_TIFRCloud', 'T3_UK_London_RHUL', 'T3_UK_SGrid_Oxford', 'T3_US_NERSC', 'T3_US_OSG', 'T3_CH_Volunteer']
print "Number of sites: %d" % len(FullSite)

# Number of time windows:
N_windows = 11
TimeSlice = range(1, N_windows)

tasks = defaultdict(list)
#path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_20170411_to_20/part-0000*'
#path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_test/part-000*'
path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_20170411_to_30/part-000*'
filelist = glob(path)
flat_input = []

# Get all input into one list
for inFileName in filelist:
    sys.stderr.write("Processing file %s\n" % inFileName)
    inFile = open(inFileName,'r')
    for sublist in inFile.readlines():
        flat_input.append(sublist)
    inFile.close()

# Read each element in the list as a dict and merge by "task"
for tmpline in flat_input:
    line = tmpline.replace("\\n","").replace("u\'","\"").replace("\'","\"")
    workflow = leval(line) # Read as a dict
    tasks[workflow["task"]].append(workflow["steps"])

# Trivial check to see when is the earliest workflow of this sample. 
mintime = 999999999999
maxtime = 0

# Delta t
deltat = 120 # 2 mins for now


with h5py.File("output/all.h5","w") as h5f:
    
    for task in tasks: 
        # Create a pandas frame with index = sites, colums = exit code. The last column is the fraction of error per site
        error = np.zeros(shape=(len(FullSite),len(FullExitCode),len(TimeSlice)+1))
        
        # Loop through different set of steps (from different tasks under the same name)
        for sets in tasks[task]:
            # Loop through steps of each sets (cmsRun, logArch, stageOut)
            for step in sets:
                if step["name"] == "cmsRun1":
                    if step["start"] < mintime and step["start"] is not None:
                        mintime = step["start"]   # After the loops will have starting time  t0
                    if step["start"] > maxtime and step["start"] is not None:
                        maxtime = step["start"]   # Check if min and max are much different
    #            if step["site"] not in n_sites: n_sites.append(step["site"])
    #            if step["exitCode"] not in n_codes: n_codes.append(step["exitCode"])
        
        # Now we have the dimension, construct the table


        # List to hold the mean failure fraction for each time window
        MeanFailure = []
        FirstIndex = 0

        #task_filename = re.findall('\/(.*?)\/',task)[0] # simplify the task name to save to file
        task_filename = task.replace('/','',1).replace('/','.') # for file name
#        if len(task_filename) > 220: 
#            print "%s is too long" %task_filename
#            task_filename = task_filename[:220] # Linux can't handle file name too long
#            print "Shorten it to: %s" % task_filename
        
        for sets in tasks[task]:
            for step in sets:
                for n in range(1,N_windows):
                    if (step["stop"] is None or step["start"] is None): continue
                    #if step["name"] == "cmsRun1" and step["start"] > (maxtime - 3600) and step["stop"] < (maxtime - 3600 + n*deltat): # for now don't care about logArch or stageOut because time stamp is complicated (some don't have start or stop time) # All jobs finished in one absolute time interval (ie., 2017/04/11 from 4 to 5 am)
                    if step["name"] == "cmsRun1" and (step["stop"] - step["start"] < n*deltat): # for now don't care about logArch or stageOut because time stamp is complicated (some don't have start or stop time) # All jobs finished in a relative time interval (ie., within 2 minute)
    #                    print("Adding one to [%s,%d]" % (step["site"],step["exitCode"]))
    #                    print("Index of these are [%d,%d]" % (FullSite.index(step["site"]),FullExitCode.index(step["exitCode"])))
                        error[FullSite.index(step["site"]),FullExitCode.index(step["exitCode"]),TimeSlice.index(n)] += 1
                                    
                if step["name"] == "cmsRun1" and step["site"] is not None and step["exitCode"] is not None: # last layer = ground truth
                    error[FullSite.index(step["site"]),FullExitCode.index(step["exitCode"]),len(TimeSlice)] += 1
            
        if error.any():  # Only save non-empty 
            #grp = h5f.create_group(task_filename)
            h5f.create_dataset(task_filename, data=error)  

            
            #if True: # Save all arrays (as opposed to only saving non-empty arrays)   
    #            df = pd.DataFrame(error, index = FullSite, columns = FullExitCode)
    #        
    #            # Fill the last column with fraction of error
    #            df["FailureRate"] = df.apply(lambda row: (row.sum()-row.loc[0])/row.sum(), axis = 1)
    #            
    #            # Mean failure rate of the workflow:
    #            MeanFailure.append(df["FailureRate"].mean())
    #
    #            #print task_filename
    #            print task
    #            print df
    #            print   
    #
    #            if not os.path.isdir("output/%ddt" %n):
    #                os.makedirs("output/%ddt" %n)
    #            df.to_csv("output/%ddt/%s.csv" %(n,task_filename), na_rep='NaN')
    #
    #    if len(MeanFailure) > 0:
    #        print task
    #        print MeanFailure
    #        plt.plot(np.arange(FirstIndex,len(MeanFailure)+FirstIndex), MeanFailure)
    #        plt.gca().set_ylim([-0.1,1.2])
    #        plt.gca().set_xlim([0,N_windows])
    #        plt.xlabel(r'$\Delta$t = %d s' %deltat)
    #        plt.ylabel('Mean Failure Rate Over Sites')
    #        plt.title(task_filename)
    #        if not os.path.isdir("output/fig"):
    #            os.makedirs("output/fig")
    #        plt.savefig("output/fig/%s.png" % task_filename)
    #        plt.close() 

print "mintime %d" %mintime
print "maxtime %d" %maxtime
print "diff %f hours" %((maxtime-mintime)/3600)

