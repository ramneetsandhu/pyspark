# Databricks notebook source
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

ssc = StreamingContext(sc, 10)
socket_stream = ssc.socketTextStream("127.0.0.1", 5555)

# COMMAND ----------

# sc = SparkContext(appName="PythonStreamingQueueStream")


# COMMAND ----------

ssc = StreamingContext(sc, 1)
rddQueue = []
for i in range(5):
    rddQueue += [ssc.SparkContext.parallelize([j for j in range(1, 1001)], 10)]

    inputStream = ssc.queueStream(rddQueue)
    mappedStream = inputStream.map(lambda x: (x % 10, 1))
    reducedStream = mappedStream.reduceByKey(lambda a,b : a + b)
    reducedStream.pprint()

    ssc.start()
    time.sleep(6)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)



# COMMAND ----------

