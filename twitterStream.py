from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("/home/mahesh/NCSU_Studies/BI/02.Apache.Spark.Streaming.Project-2.SentimentAnalysis.FINAL/positive.txt")
    nwords = load_wordlist("/home/mahesh/NCSU_Studies/BI/02.Apache.Spark.Streaming.Project-2.SentimentAnalysis.FINAL/negative.txt")
    
    pwords = pwords[:-1]
    nwords = nwords[:-1]
    
    counts = stream(ssc, pwords, nwords, 100)
    #print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    plt.figure(1)
    posArr = []
    negArr = []
    for j in [i for i in counts if len(i)>1]:
        posArr.append(j[0][1])
        negArr.append(j[1][1])

    print('POS -',posArr)
    print('Neg -',negArr)
    plt.plot(range(1,len(posArr)+1), posArr, color='blue', label='positive',linestyle='solid', marker='o',markerfacecolor='blue', markersize=8)
    plt.plot(range(1,len(posArr)+1), negArr, color='green', label='negative',linestyle='solid', marker='o',markerfacecolor='green', markersize=8)

    plt.legend('upper left')
    plt.axis([0,len(posArr)+1,0,50+max([max(posArr),max(negArr)])])
    plt.xlabel('Time step')
    plt.ylabel('World count')

    plt.savefig('myfig')
    plt.close()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    return open(filename,'r').read().split('\n')
    


def updateFunc(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    #tweets.pprint()

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    words = tweets.flatMap(lambda line: line.split(" "))
    
    #posWordsCount = words.map(lambda word: ('positive',1) if word in pwords else (word,1)).map(lambda wordTup: ('negative',1) if (wordTup[0] != 'positive'\
    #     and wordTup[0] in nwords) else ('',0)).reduceByKey(lambda a, b: a+b)
    posWordsCount = words.map(lambda word:('positive'*(word in pwords)+'negative'*(word in nwords),1)).\
    reduceByKey(lambda a, b: a+b).filter(lambda data: data[0]!='' and data[0] != 'positivenegative')
    #posWordsCount.pprint()
        
    '''
    print('Length of pwords:',len(pwords),len(set(pwords)))
    print('Length of nwords:',len(nwords),len(set(nwords)))
    #print('Length of the intersection is:-----------------------------------------------------',set(pwords).intersection(nwords))
    '''    
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    posWordsCount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    runningCounts = posWordsCount.updateStateByKey(updateFunc)
    runningCounts.pprint()

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()

'''
-----------------------------Output:-----------------------------
-------------------------------------------
Time: 2017-01-18 18:08:50
-------------------------------------------
('positive', 166)
('negative', 65)

-------------------------------------------
Time: 2017-01-18 18:09:00
-------------------------------------------
('positive', 283)
('negative', 131)

-------------------------------------------
Time: 2017-01-18 18:09:10
-------------------------------------------
('positive', 413)
('negative', 201)

-------------------------------------------
Time: 2017-01-18 18:09:20
-------------------------------------------
('positive', 550)
('negative', 282)

-------------------------------------------
Time: 2017-01-18 18:09:30
-------------------------------------------
('positive', 680)
('negative', 351)

-------------------------------------------
Time: 2017-01-18 18:09:40
-------------------------------------------
('positive', 868)
('negative', 431)

-------------------------------------------
Time: 2017-01-18 18:09:50
-------------------------------------------
('positive', 1017)
('negative', 502)

-------------------------------------------
Time: 2017-01-18 18:10:00
-------------------------------------------
('positive', 1153)
('negative', 585)

-------------------------------------------
Time: 2017-01-18 18:10:10
-------------------------------------------
('positive', 1280)
('negative', 651)

-------------------------------------------
Time: 2017-01-18 18:10:20
-------------------------------------------
('positive', 1438)
('negative', 721)

-------------------------------------------
Time: 2017-01-18 18:10:30
-------------------------------------------
('positive', 1593)
('negative', 812)

-------------------------------------------
Time: 2017-01-18 18:10:40
-------------------------------------------
('positive', 1720)
('negative', 882)


'''