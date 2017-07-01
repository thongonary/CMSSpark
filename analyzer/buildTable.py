import json
import numpy as np
import pandas as pd
from collections import defaultdict
from glob import glob
from ast import literal_eval as leval 
import math
import re 
import os, sys

tasks = defaultdict(list)
#path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_20170411_to_20/part-0000*'
path = '/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_test/part-000*'
filelist = glob(path)
pathwaylist = []

for inFileName in filelist:
    sys.stderr.write("Processing file %s\n" % inFileName)
    inFile = open(inFileName,'r')
    pathwaylist.append(inFile.readlines())
    inFile.close()

# Flatten the input list
flat_input = [item for sublist in pathwaylist for item in sublist]

# Read each element in the list as a dict and merge by "task"
for tmpline in flat_input:
    line = tmpline.replace("\\n","").replace("u\'","\"").replace("\'","\"")
    workflow = leval(line)
    tasks[workflow["task"]].append(workflow["steps"])


#with open('/afs/cern.ch/work/q/qnguyen/public/CMSSpark/wma_test/whole') as data_file: 
#    for tmpline in data_file:
#        line = tmpline.replace("u\'","\"").replace("\'","\"")
#        workflow = leval(line)
#        tasks[workflow["task"]].append(workflow["steps"])

# Check earliest time start to set t0
mintime = 999999999999
maxtime = 0

# Delta t
deltat = 3600 # 1 hour for now

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
    # Create a pandas frame with index = sites, colums = exit code
    error = np.zeros(shape=(len(n_sites),len(n_codes)))

    # Make 1h, 2h, 3h table:
    for n in range(1,10):
        for sets in tasks[task]:
            for step in sets:
                if step["name"] == "cmsRun1" and step["start"] > (mintime+(maxtime-mintime)/4) and step["start"] < (mintime+(maxtime-mintime)/4+n*deltat): # for now don't care about logArch or stageOut because time stamp is complicated
                    error[n_sites.index(step["site"]),n_codes.index(step["exitCode"])] += 1

        if (error.any()): # Do not save empty array
            df = pd.DataFrame(error, index = n_sites, columns = n_codes)
            task_filename = re.findall('\/(.*?)\/',task)[0]
            print task_filename 
            print df
            print   

            if not os.path.isdir("output/%dh" %n):
                os.makedirs("output/%dh" %n)
            df.to_csv("output/%dh/%s.csv" %(n,task_filename), na_rep='NaN')

print "mintime %d" %mintime
print "maxtime %d" %maxtime
print "diff %f hours" %((maxtime-mintime)/3600)
