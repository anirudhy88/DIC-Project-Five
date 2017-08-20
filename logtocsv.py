
import re
import numpy as np

import csv
data=[0,0,0,0]

with open("logfile", "r") as ins:       #input put file
    array = []
    for line in ins:
        array.append(line)
        

final_data = []


def time_conv(dat):
    minute , sec = re.split(r"m", dat)
    sec = re.sub(r"s","",sec)
    min_to_sec = float(minute)*60
    return float(sec) + min_to_sec

j=0;
result=[]
for i in range(0,len(array)):
    if(re.match(r"-",array[i])):
        num = array[i+1].rstrip()
        real = array[i+2].split("\t")[1].rstrip()
        j=j+1
      

        data[0] = int(num)
        
        data[j] = time_conv(real)
        if(j==2):
             final_data.append(data)
             print data
             data = [0,0,0,0]
             j=0
                
with open('logfile.csv', 'w') as fp:      # outputfile as csv
    a = csv.writer(fp, delimiter=',')
    
    a.writerows(final_data)
    print final_data
    
fp.close()
