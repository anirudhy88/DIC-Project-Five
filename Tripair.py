import pyspark
import csv
import re
import itertools
import datetime
import time
import sys



reader = csv.reader(open('new_lemmatizer.csv'))
result = {}
l = []
for row in reader :
        key = row[0]
        #print key
        if key in result:
                pass
        s = row[1:]
        l[:] = []
        for i in range(0, len(s)-1) :
          if s[i] != "" :
            l.append(s[i])
        #print l
        result[key] = l[0:]
  #print result
#print result["v"]
def function (text):
  l=[]
  if text in result:
    return result[text]
  else:
    l.append(text)
    return l
def function1 (text) :
  s = re.search('<(.+?)>',text)
  d = re.search('\d',text)
  if s:
    return s.group(0)
  elif d: 
    return unicode("<")+text[0:d.end()+1]+unicode(">")
  else :
    return unicode("<")+text[0:10]+unicode(">")
    

sc = pyspark.SparkContext()
text_file = sc.textFile("data/")
counts = text_file.glom() \
	.flatMap(lambda x: x ) \
    .map(lambda x: (re.sub(ur"[^\w\d'\s]+",'',x.replace(function1(x),'').strip().lower()),function1(x))) \
    .flatMap(lambda x: [((list(x)[0].split()[index],list(x)[0].split()[index+1],list(x)[0].split()[index+2]),list(x)[1]) for index in range(0,len(list(x)[0].split())-2)]) \
    .map(lambda x: [(y,list(x)[1]) for y in list(itertools.product(function(list(list(x)[0])[0].replace('j','i').replace('v','u')),function(list(list(x)[0])[1].replace('j','i').replace('v','u')),function(list(list(x)[0])[2].replace('j','i').replace('v','u'))))]) \
    .flatMap(lambda x:x) \
    .reduceByKey(lambda x,y:x+","+y) \

counts.map(lambda x:"("+list(list(x)[0])[0]+","+list(list(x)[0])[1]+","+list(list(x)[0])[2]+")"+" "+"---->"+" "+"{ "+"Count = "+str(len(str(list(x)[1]).split(",")))+" } "+str(list(x)[1])).saveAsTextFile("output_tripair/"+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))







