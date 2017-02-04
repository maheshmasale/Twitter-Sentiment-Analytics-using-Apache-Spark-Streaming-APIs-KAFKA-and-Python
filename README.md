# Twitter-Sentiment-Analytics-using-Apache-Spark-Streaming-APIs-KAFKA-and-Python

In this project, I learnt about processing live data streams using Spark’s streaming APIs
and Python. I performed a basic sentiment analysis of realtime tweets. In addition,
I also got a basic introduction to Apache Kafka, which is a queuing service for data streams.

One of the first requirements is to get access to the streaming data; in this case, realtime tweets. 
Twitter provides a very  convenient API to fetch tweets in a streaming manner. Start by reading the basic documentation
found here.
In addition, you will also be using Kafka to buffer the tweets before processing. Kafka provides a
distributed queuing service which can be used to store the data when the data creation rate is
more than processing rate. It also has several other uses. Read about the basics of Kafka
( Introduction up to Quick Start ) from the official documentation .
Finally, as an introduction to stream processing API provided by Spark, read the p rogramming
guide from the beginning up to, and including, the section on discretized streams (DStreams).
Pay special attention to the “quick example” as it will be fairly similar to this project.
