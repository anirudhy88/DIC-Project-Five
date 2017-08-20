#!/bin/bash
#Author © Madhav Jakkampudi 

TOTAL=$1            # First command line argument
N=$2                # Second command line argument
VAR=$((TOTAL/N))
echo "($VAR)"
mv data/* latin/
mv logfile logfile_backup
#hdfs dfs -rmr ~/output*
for((j=1;j<=VAR;++j)); do
    

	for((i=0;i<N;++i)); do
    	files=(latin/*) && mv "${files[RANDOM % ${#files[@]}]}" data/
	done
	echo "------------------------------------------------------------------------------------" >> logfile
    echo "$((j*N))" >> logfile
    (time python wordpairmain.py) 2>&1 | tail -3 >>logfile   #last three lines of output into logfile
    echo "------------------------------------------------------------------------------------" >> logfile
    echo "$((j*N))" >> logfile
    (time python Tripair.py) 2>&1 | tail -3 >>logfile
    echo "($j)"   

done
python logtocsv.py
