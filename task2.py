import argparse
import json
import pandas as pd
import os
import fnmatch
import time
from pathlib import Path
from pandas.io.json import json_normalize
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join

start_time = time.time()

parser = argparse.ArgumentParser()

parser.add_argument("path",help="Enter the files path")
parser.add_argument("-u","--reem", action="store_true", dest="timestamp", default=False,help="check time stamp")

args = parser.parse_args()
path =  args.path

files = [item for item in listdir(path) if isfile(join(path, item)) if fnmatch.fnmatch(item, '*.json')]
checksums={}
duplicates=[]
for filename in files:
    with Popen(["md5sum", path + '/' + filename], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]
        if checksum in checksums:
            os.remove(path + '/' + filename)
            duplicates.append(filename)
        checksums[checksum] = filename

uniqueFiles = list(set(files) - set(duplicates))
print(f"Found Duplicates: {duplicates}")



for filename in uniqueFiles:

    records = [json.loads(line) for line in open(path + '/' + filename)]
    data = json_normalize(records)
    
    data = data[["a","r","u","cy","ll","tz","t","hc"]]
    data['web_browser'] = data["a"].str.split('(').str[0]
    data['operating_sys'] = data["a"].str.split('(').str[1]
    data['operating_sys'] = data["operating_sys"].str.split(' ').str[0]
    data['operating_sys'] = data["operating_sys"].str.split(';').str[0]
    
    data['from_url'] = data["r"].str.split('http://').str[1]
    data['from_url'] = data["from_url"].str.split('/').str[0]
    
    data['to_url'] = data["u"].str.split('http://').str[1]
    data['to_url'] = data["to_url"].str.split('/').str[0]
    
    data = data.dropna(axis=0)
    data[['longitude','latitude']] = pd.DataFrame(data["ll"].tolist(), index = data.index)
    
    data = data.rename(columns={'tz':'time_zone','t':'time_in','hc':'time_out','cy':'city'})
    data = data[["web_browser","operating_sys","from_url","to_url","city","longitude","latitude","time_zone","time_in","time_out"]]
    
    #print(data)
    
    no_rows = data.shape[0]
    print('Number of Rows' + str(no_rows))
    
    if not args.timestamp:
        data['time_in'] = pd.to_datetime(data.time_in, unit='s')
        data['time_out'] = pd.to_datetime(data.time_out, unit='s')

        data = data.dropna(axis=0)
        

        data.to_csv('/home/reemelmadboly/ITI_Python_for_Data_Management/Task2/target' + '/' + filename + '.csv', index=False)
        os.rename(path + '/' + filename , path + '/' + filename + '.complete')
    
    else:
    	
    	data = data.dropna(axis=0)
    	data.to_csv('/home/reemelmadboly/ITI_Python_for_Data_Management/Task2/target' + '/' + filename + '.csv', index=False)
    	os.rename(path + '/' + filename , path + '/' + filename + '.complete')
        
         
    
total_time = time.time() - start_time
print('Total Time' + str(total_time))
