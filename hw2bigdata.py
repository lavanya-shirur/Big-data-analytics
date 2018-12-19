from __future__ import print_function
from pyspark import SparkConf, SparkContext
from operator import add
import ntpath
import re
from operator import itemgetter
import nltk
from nltk.corpus import stopwords
import itertools
import os
import time


conf = SparkConf()
conf.setAppName("WordCount")
conf.set("spark.executor.memory","8g")
conf.set('spark.kryoserializer.buffer.max', "2047")
sc = SparkContext(conf=conf)
Output = sc.emptyRDD()
print("start time is")
print(time.ctime())
#Text Preprocessing
def PreProcess(text):
    return re.sub('[^a-z| |0-9]', '', text.strip().lower())


#PART A - WORD COUNT
#Wordcount flliior each file
def WordcountForEachFile(filename):
    SimpleFileName = ntpath.basename(filename)
    InputText = sc.textFile(filename)
    Words = InputText.flatMap(lambda line: line.split())
    #print(Words.take(1))
    PreProcessed_Words = Words.map(PreProcess)
    WordCounts = PreProcessed_Words.map(lambda w: (w, 1))
    WordCounts_AfterReduceByKey = WordCounts.reduceByKey(add, numPartitions=1)
    Filename_WordCounts = WordCounts_AfterReduceByKey.map(lambda x: (SimpleFileName, x))
    #print(Filename_WordCounts.collect())
    return Filename_WordCounts


#reads a RDD with each entry as a whole file
corpus = sc.wholeTextFiles("/cosc6339_hw2/gutenberg-22/")

List_FileNames = corpus.map(lambda (filename, content) : filename)
for Filename in List_FileNames.collect():
    tmp = WordcountForEachFile(Filename)
    Output = Output.union(tmp)

#GroupBy FileName and form a list of words & its count
GroupByFilename_WordCount = Output.groupByKey().mapValues(lambda x: list(x))
newlist2=[]
newlist3=[]
flat_list=[]
newstr_list=[]
newlistxt=[]

from collections import defaultdict
wc_results = defaultdict(list)

for i in range(0,len(GroupByFilename_WordCount.collect())):
        newlist2.append(GroupByFilename_WordCount.collect()[i][1])
        #list of all .txt files
        newlistxt.append(GroupByFilename_WordCount.collect()[i][0])
        wc_results[GroupByFilename_WordCount.collect()[i][0]].append(GroupByFilename_WordCount.collect()[i][1])

total_words = {}
for key,value in wc_results.iteritems():
        total_word_count = 0
        for wc in value[0]:
                total_word_count = total_word_count + wc[1]
        total_words[key] = total_word_count


#apend list of list into a single list
for i in range(0,len(newlist2)):
        newlist3.append(newlist2[i])
flat_list = [item for sub in newlist3 for item in sub]

#for converting unicode to normal text
for i in range(0,len(flat_list)):
        tup=(str(flat_list[i][0]),flat_list[i][1])
        newstr_list.append(tup)

#for removing stopwords
fil=[]
newfil=[]
for i in range(0,len(newstr_list)):
        filtered_words = list(filter(lambda word: word not in stopwords.words('english'), newstr_list[i]))
        fil.append(filtered_words)

#for removing list with one value
for i in fil:
        if (len(i)>1):
                newfil.append(i)

#sorting
newfil.sort()
newfil2=list(newfil for newfil,_ in itertools.groupby(newfil))
sortedlist=sorted(newfil2,key=lambda x:x[1],reverse=True)
#print ("final words list")
new_items=[]
word_list2=[]
final_list=[]
for i in range(0,len(sortedlist)):
        if sortedlist[i][0] not in word_list2:
                word_list2.append(sortedlist[i][0])
new_items = [item for item in word_list2 if not item.isdigit()]
final_list=new_items[0:1000]
#Part B : INVERTED INDEX
from collections import defaultdict
submission_list = defaultdict(list)

inverted_index=[]
weight_list=[]
for wordindex,word in enumerate(final_list):
        for textindex,text in enumerate(List_FileNames.collect()):
                index=[0.0]*len(List_FileNames.collect())
                tmp,uni = os.path.split(text)
                if uni in wc_results:
                        for wc in wc_results[uni][0]:
                                if word == wc[0]:
                                        index[textindex]=float(wc[1])/float(total_words[uni])
                                        weight_list=float(wc[1])/float(total_words[uni])
                                        #print(word,uni,weight_list)
                        submission_list[word].append([uni,weight_list])

print(submission_list)


#PART C - SIMILARITY MATRIX

flatS=[]
S=[]
for textindex1,text1 in enumerate(List_FileNames.collect()):
        s=[]
        for textindex2,text2 in enumerate(List_FileNames.collect()):
                if textindex1 != textindex2:
                        similarity = 0.0
                        for wordindex, word in enumerate(final_list):
                                weight1 = 0.0
                                weight2 = 0.0
                                if submission_list[word]:
                                        tmp,uni1 = os.path.split(text1)
                                        tmp,uni2 = os.path.split(text2)
                                        for item in submission_list[word]:

                                                if item[0]==str(uni1):
                                                        weight1 = item[1]
                                                if item[0]==str(uni2):
                                                        weight2= item[1]
#                               print(weight1,weight2)
                                similarity = similarity + (float(weight1)*float(weight2))
                        s.append(similarity)
                        flatS.append(similarity)
        S.append(s)
#       flatS.append(s)
print(S)

flatS.sort()
print("flatS")

print(flatS)

#sortedfile=sorted(float(flatS),key=lambda x:x[1],reverse=True)
#sortedfiles=float(sortedfile[0:10])
flatS=flatS[::-10]
for i in flatS:
        for j in range(0,len(S)):
                for k in range(0,j):
                        if i == S[j][k]:
                                print (j,k)
print(List_FileNames.collect()[j],List_FileNames.collect()[k])

print("end time")
print(time.ctime())
newinv=[]


