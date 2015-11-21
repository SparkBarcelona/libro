from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

if __name__ == "__main__":
    sc = SparkContext()
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream("localhost", 9999)
    running_counts = lines.flatMap(lambda line: line.split(" "))\
                          .map(lambda word: (word, 1))\
                          .updateStateByKey(updateFunc)
    running_counts.pprint()
    ssc.start()
    ssc.awaitTermination()

