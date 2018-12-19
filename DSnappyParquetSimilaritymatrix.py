from pyspark import SparkConf, SparkContext
from operator import add
import time
import ast
import re
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


# Load inverted index from a saved text file
def loadInvertedIndex(inputFile):
        text = sc.textFile(inputFile)
        wordCountPerFile = text.flatMap(lambda line: [ast.literal_eval(line)]).map(lambda w: w)
        return wordCountPerFile

def getWordsimPerTwoFile(weightsDict):
        simPerTwoFileList=list()
        keysList = weightsDict.keys()
        for k, v in weightsDict.items():
                keysList.remove(k)
                for otherKey in keysList:
                        key = k + ":" + otherKey if k < otherKey else otherKey + ":" + k
                        value = weightsDict[k]*weightsDict[otherKey]
                        simPerTwoFileList.append((key, value))
        return simPerTwoFileList

def calSimilMatrix(wordCountPerFile):
#       simMatrix = wordCountPerFile(lambda x : getWordsimPerTwoFile(x[1])).map(lambda x:x).reduceByKey(add)
        simMatrix = wordCountPerFile.flatMap(lambda x : getWordsimPerTwoFile(x[1])).map(lambda x:x).reduceByKey(add)
#       simMatrix.saveAsTextFile(outputDir)
        return simMatrix


if __name__ == '__main__':
        conf = SparkConf()
        conf.setAppName("Files_similarity")
        conf.set("spark.executor.memory", "4g")
        sc = SparkContext(conf = conf)
        sqlContext = SQLContext(sc)
        spark = SparkSession(sc)

        inputFile = "/bigd27/PartDInverted_IndexFinal.parquet"


        #READ INVERTED INDEX FORM PARQUET FILE
        t1=time.time()
        print "Load Inverted Index from",inputFile
        items = sqlContext.read.parquet("/bigd27/PartDInverted_IndexFinal.parquet")

        #CALCULATE SIMILARITY MATRIX
        itemsrdd=items.rdd
        print "Cal similarity matrix and save to"
        simMatrix = calSimilMatrix(itemsrdd)
        new = simMatrix.toDF()
        new.write.parquet("/bigd27/PartDSimilarity_MatrixFinal.parquet")

        # READ FROM PARQUET FILE AND DISPLAY
        #       finaloutput = sqlContext.read.parquet("/bigd27/PartCInverted_IndexFinal.parquet")
        t2 = time.time()
        print "SimilMatrix finished in %.2f" % (t2 - t1)