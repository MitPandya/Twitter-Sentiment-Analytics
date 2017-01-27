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
    	
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    #print "positive word count : "+str(len(pwords))
    #print "negative word count : "+str(len(nwords))
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    #print counts
    # Create empty list of pos and neg
    pos = []
    neg = []
    # Iterate through the list of counts
    for i in range(len(counts)):
    # ignore if count object is empty
	if counts[i] <> []:
	   # add word counts to pos and neg list for plotting	
	   pos.append(counts[i][0][1])
	   neg.append(counts[i][1][1])
	# Create a subplot
    ax = plt.subplot(111)
    t1 = np.arange(0, len(pos),1)
    # plot positive and negative words
    ax.plot(t1,pos,'bo-',label = "positive")
    ax.plot(t1,neg,'go-', label = "negative")
    # Find range of y and add 50 for legend space
    y_max = max(max(pos),max(neg))+50
    #ax.set_ylim(-1,y_max)
    #ax.set_xlim(-1,len(pos))
    # Set axis range
    ax.axis([-1,len(pos),-1, y_max])
    # Apply x and y labels
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    # Add legend to upper left corner of plot
    plt.legend(fontsize = 'small',loc='upper left')
    # Save plot as 'plot.png'
    plt.savefig("plot.png")
    plt.show()

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    wordList = []
    # Read the file
    file = open(filename, 'r')
    # For every line in file
    for line in file:
       # Remove tariling white characters 
       word = line.strip()
       # If duplicate then ignore
       if not word in wordList:
          wordList.append(word)
    return wordList

def updateFunction(newValues, runningCount):
    if runningCount == None:
       runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    # Split the tweet rdd into words
    words = tweets.flatMap(lambda x: x.split(" "))
    # If word is positive add it in the list as 'positive' with count 1
    positiveWords = words.filter(lambda x: x in pwords).map(lambda x: ("positive",1))
    # Combine all positive words and increment their count
    positiveWordCounts = positiveWords.updateStateByKey(updateFunction).reduceByKey(lambda x, y : x + y)
    # If word is negative add it in the list as 'negative' with count 1
    negativeWords = words.filter(lambda x: x in nwords).map(lambda x: ("negative",1))
    # Combine all negative words and increment their count
    negativeWordCounts = negativeWords.updateStateByKey(updateFunction).reduceByKey(lambda x, y : x + y)
    # Merge positive and negative word counts
    combinedWordCounts = positiveWordCounts.union(negativeWordCounts)
    
    # positiveWordCounts.pprint()
    # negativeWordCounts.pprint()
    combinedWordCounts.pprint()
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    # Positive and Negative words without combining all states
    positiveWords = positiveWords.reduceByKey(lambda x,y : x + y)
    negativeWords = negativeWords.reduceByKey(lambda x,y : x + y)
    # merge positive and negative counts
    allCounts = positiveWords.union(negativeWords)
    # For each rdd of positive and negative counts iterate them and add it to a list
    allCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
