LAB - 5
-------
Teammates : 
1. Hema Madhav Jakkampudi-50206563
2. Anirudh Yellapragada-50206970

Below are the commands to be given from the current path for both Bi-pair and Tri-pair.


1.Run the python file using below command : 
python <file-name>
2. File used for lemmatization is : new_lemmatizer.csv 
3. "data" folder consists of input files.
4. "data" folder is mentioned in the programs of Bi-pair and Tri-pair.
5. The resultant output files are written to "output_bipair” and “output_tripair” folder.

We have implemented word-cooccurence (bi-pair and tri-pair) in spark framework using python. Referred sources includes examples folder of the spark. 

We ran the bi-pair program for over 200 documents(starting from 10 and increasing in steps of 10).
We ran the tri-pair program for over 200 documents(starting from 10 and increasing in steps of 10).

We have written a small script “timeshell.sh” which does the above and logs the time taken into “logfile.csv”


Observations :
The time taken by the spark program increases almost linearly with the number of documents both in 2-gram and 3-gram cases. However, the Bi-Pair takes less time when compared to Tri-Pair with same number of documents.

Running scripts : 
python wordpairmain.py
python Tripair.py

To run the time script :
./timeshell.sh [total no.of files] [in the splits of]
examples :
./timeshell.sh 200 10

PS : The shell script randomly picks files from “latin” folder and places them into 
“data” folder and runs the main python files which take data from “data” folder.

logtocsv.py converts generated logfile to csv.
