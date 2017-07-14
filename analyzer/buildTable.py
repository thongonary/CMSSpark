import json
import numpy as np
import pandas as pd
from collections import defaultdict
from glob import glob
from ast import literal_eval as leval 
import math
import re 
import os, sys
import matplotlib.pyplot as plt

tasks = defaultdict(list)
#path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_20170411_to_20/part-0000*'
path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_test/part-000*'
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
deltat = 60 # 1 mins for now

for task in tasks: 
    
    # First need to know array size: Number of sites, number of exit code
    n_sites = []
    n_codes = []

    # Loop through different set of steps (from different tasks under the same name)
    for sets in tasks[task]:
        # Loop through steps of each sets (cmsRun, logArch, stageOut)
        for step in sets:
            if step["name"] == "cmsRun1":
                if step["start"] < mintime and step["start"] is not None:
                    mintime = step["start"]   # After the loops will have starting time  t0
                if step["start"] > maxtime and step["start"] is not None:
                    maxtime = step["start"]   # Check if min and max are much different
            if step["site"] not in n_sites: n_sites.append(step["site"])
            if step["exitCode"] not in n_codes: n_codes.append(step["exitCode"])
    
    # Now we have the dimension, construct the table
    # Create a pandas frame with index = sites, colums = exit code. The last column is the fraction of error per site
    error = np.zeros(shape=(len(n_sites),len(n_codes)))

    # Number of time windows:
    N_windows = 100

    # List to hold the mean failure fraction for each time window
    MeanFailure = []
    FirstIndex = 0
    #task_filename = re.findall('\/(.*?)\/',task)[0] # simplify the task name to save to file
    task_filename = task.replace('/','_') # for file name
    if len(task_filename) > 220: task_filename = task_filename[:220] # Linux can't handle file name too long

    # Make N tables
    for n in range(1,N_windows):
        for sets in tasks[task]:
            for step in sets:
                if step["name"] == "cmsRun1" and step["start"] > (maxtime - 36000) and step["start"] < (maxtime - 36000 + n*deltat): # for now don't care about logArch or stageOut because time stamp is complicated (some don't have start or stop time)
                    error[n_sites.index(step["site"]),n_codes.index(step["exitCode"])] += 1
        
        if not error.any(): FirstIndex = n  # There might be empty list when the time window is too small, skip those
        else: # Only save if the np array is not empty
            
            df = pd.DataFrame(error, index = n_sites, columns = n_codes)
        
            # Fill the last column with fraction of error
            df["FailureRate"] = df.apply(lambda row: (row.sum()-row.loc[0])/row.sum(), axis = 1)
            
            # Mean failure rate of the workflow:
            MeanFailure.append(df["FailureRate"].mean())

            #print task_filename
            print task
            print df
            print   

            if not os.path.isdir("output/%ddt" %n):
                os.makedirs("output/%ddt" %n)
            df.to_csv("output/%ddt/%s.csv" %(n,task_filename), na_rep='NaN')

    if len(MeanFailure) > 0:
        print task
        print MeanFailure
        plt.plot(np.arange(FirstIndex,len(MeanFailure)+FirstIndex), MeanFailure)
        plt.gca().set_ylim([-0.1,1.2])
        plt.gca().set_xlim([0,N_windows])
        plt.xlabel(r'$\Delta$t = %d s' %deltat)
        plt.ylabel('Mean Failure Rate Over Sites')
        plt.title(task_filename)
        if not os.path.isdir("output/fig"):
            os.makedirs("output/fig")
        plt.savefig("output/fig/%s.png" % task_filename)
        plt.close() 

print "mintime %d" %mintime
print "maxtime %d" %maxtime
print "diff %f hours" %((maxtime-mintime)/3600)

