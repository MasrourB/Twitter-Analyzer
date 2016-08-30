package Main
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import TwitterAuth.Auth

/**
 * Takes in 4 VM arguments regarding Twitter Developer account credentials
 * Reads in a stream of tweets from twitter and aggregates the data using Spark Streaming.
 * Transforms the data to reveal the most popular hashtags from the last 10 seconds and most influential tweeters
 */
object Main {
  def main(args: Array[String]) {
    

    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("local[*]")
val sc = new SparkContext(config)

    
    sc.setLogLevel("WARN")

val ssc = new StreamingContext(sc, Seconds(10))
    val consumerKey = System.getProperty("consumerKey");
    val consumerSecret = System.getProperty("consumerSecret");
    val accessToken = System.getProperty("accessToken");
    val accessTokenSecret = System.getProperty("accessTokenSecret");
    
    val auth = new Auth(consumerKey, consumerSecret, accessToken, accessTokenSecret)
   

System.setProperty("twitter4j.oauth.consumerKey", auth.consumerKey)
System.setProperty("twitter4j.oauth.consumerSecret", auth.consumerSecret)
System.setProperty("twitter4j.oauth.accessToken", auth.accessToken)
System.setProperty("twitter4j.oauth.accessTokenSecret", auth.accessTokenSecret)

//Start the twitter stream
val stream = TwitterUtils.createStream(ssc, None)

/*
 * Start top user analysis
 */
val tweets = stream.map(status => (status.getText,status.getRetweetCount))
tweets.print

val users = stream.map(status => (status.getUser))

val topUsers = users.map(user => (user.getName,user.getFollowersCount))
val top10 = topUsers.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(10))

val topTenUsers = top10.transform(rdd => rdd.sortBy(user => user._2))

/*
 * Start hashtag analysis
 */
val hashTags = stream.flatMap(status => status.getHashtagEntities)
val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))

val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(10))

val sortedTopCounts10 = topCounts10.transform(rdd => 
  rdd.sortBy(hashtagPair => hashtagPair._2, false))
  
  
  topTenUsers.foreachRDD(rdd => {
  val topList = rdd.take(10)
  println("\nTop 10 users (%s total):".format(rdd.count()))
  topList.foreach{case (tag, count) => println("%s (%d followers)".format(tag, count))}
})
  
  sortedTopCounts10.foreachRDD(rdd => {
  val topList = rdd.take(10)
  println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
  topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
})

ssc.start()
ssc.awaitTermination()
   
  }
}