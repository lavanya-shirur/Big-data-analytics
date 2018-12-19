from pyspark import SparkConf, SparkContext
from operator import add
from nltk.corpus import stopwords
import nltk
import os
import time
import re
from pyspark.sql import SparkSession

def removeStopWords(word_list, wordRemoveList=[]):
        for word in wordRemoveList:
                if word in word_list:
                        word_list.remove(word)
        return word_list

def getCleanWordsByLine(line, wordRemoveList=[]):
        wordList = re.sub('[^A-Za-z0-9\t\n\v\f\r ]+', ' ', line).lower().split()
        wordList = removeStopWords(wordList, wordRemoveList=wordRemoveList)
        return wordList


def getCleanWordsByFile(inputData, topNItems, wordRemoveList=[]):
    wordByFileList = list()
    baseName = os.path.basename(inputData[0])
    linesList = inputData[1].splitlines()
    wordPerFile = 0
    for line in linesList:
        wordList = getCleanWordsByLine(line, wordRemoveList)
        for word in wordList:
            wordPerFile += 1
            if word in topNItems:
                wordByFileList.append((word, baseName))
    wordByFileList.append([(baseName, wordPerFile)])
    return wordByFileList

def getWordFromFile(inputData):
    wordList = list()
    for line in inputData[1].splitlines():
        wordList.append(line.split("\'")[1])
    return wordList


def loadTopNWordList(inputFile):
    text = sc.wholeTextFiles(inputFile)
    word_list = text.flatMap(lambda inputData: getWordFromFile(inputData))
    return word_list.collect()

def mapInvertedIndex(inputData, filesWordCount):
        # inputData[0][0] os the word and inputData[0][1] is the fileName & inputData[1] is wordcount
    tempDict = dict()
    tempDict[inputData[0][1]] = float(inputData[1]) / float(filesWordCount[inputData[0][1]])
    return (inputData[0][0], tempDict)


def reduceInvertedIndex(x, y):
    finalDict = x.copy()
    finalDict.update(y)
    del x
    del y
    return finalDict


def mapWordCountPerFile(inputData):
    if (isinstance(inputData, tuple)):
        return (inputData, 1)
    else:
        return ("ALLFILES", inputData)

def getInvertedIndex(inputFile, topNItems, wordRemoveList=[]):
    text = sc.wholeTextFiles(inputFile)
    wordsFlat = text.flatMap(lambda inputData: getCleanWordsByFile(inputData, topNItems, wordRemoveList))
    words = wordsFlat.map(lambda x: mapWordCountPerFile(x))
    counts = words.reduceByKey(add)
    allFiles = counts.lookup("ALLFILES")
    filesWordCount = dict()
    for file in allFiles[0]:
        filesWordCount[file[0]] = int(file[1])

    wordCountPerFile = counts.filter(lambda x: isinstance(x[0], tuple)).map(lambda x: mapInvertedIndex(x, filesWordCount)).reduceByKey(lambda x, y: reduceInvertedIndex(x, y))
    return wordCountPerFile

if __name__ == '__main__':
        conf = SparkConf()
        conf.setAppName("Files_similarity")
        conf.set("spark.executor.memory", "4g")
        sc = SparkContext(conf = conf)

        wordRemoveList = []
        nltk.download('stopwords')
        stop_words = stopwords.words('english') + wordRemoveList

        topWords_inputFile = "/bigd27/thousandwordsniv"
        inputFile = "/cosc6339_hw2/gutenberg-500"
        t1=time.time()

        print "Load Top 1000 words from", topWords_inputFile
        topNItems = loadTopNWordList(topWords_inputFile)

        print "Create Inverted Index"
        invertedindex=getInvertedIndex(inputFile, topNItems, stop_words)

        #write data using avro
        spark = SparkSession(sc)
        new=invertedindex.toDF()

        new.write.format("com.databricks.spark.avro").save("/bigd27/PartBInverted_IndexFinal.avro")
        t2=time.time()
        print "Inverted Index finished in %.2f" % (t2-t1)



