# Twitter Analyzer

A big data application that reads in a stream of the most current tweets and returns the most popular tweeters and hashtags. This is done by using Spark Streaming as a data processing engine to filter our streamed data.

What is Spark?

Spark is an open-source data processing engine developed by Apache that is designed to process large volumes of data on distributed systems. Most often this technology is used within the Hadoop framework and is much like MapReduce. Howevever, Spark is more suitable for real-time streaming of data and is much faster due to data being stored in-memory instead of on disk which is what traditional MapReduce does.

This application is designed so far to run locally on a user machine but processes the data coming in from Twitter on multiple cores. This app can be deployed to a multi-node cluster to take advantage of Distributed Systems as well for even more scaling if your twitter stream is exceptionally large. That functionality will be coming soon.

Feel free to use it to find some people you might want to follow!

