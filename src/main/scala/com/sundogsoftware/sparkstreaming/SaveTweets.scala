package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import java.util.{ArrayList, Base64}
import java.nio.charset.StandardCharsets
import scala.io.Source
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import java.io.File
import Utilities._

/**
 * Object SaveTweets
 * This object handles fetching tweets from Twitter's API using Spark Streaming
 * and saving them to disk. It includes methods for obtaining Twitter credentials,
 * generating a bearer token, and processing tweets in real-time.
 */
object SaveTweets {

  /**
   * Reads Twitter API credentials from a file.
   * @return A tuple containing the API key and API secret.
   * @throws Exception if the file is not found or credentials are missing.
   */
  def getTwitterCredentials(): (String, String) = {
    val file = new File("data/twitter.txt")
    if (!file.exists()) throw new Exception("File data/twitter.txt not found")

    val lines = Source.fromFile(file).getLines().toList
    val key = lines.find(_.startsWith("consumerKey=")).map(_.split("=")(1)).orNull
    val secret = lines.find(_.startsWith("consumerSecret=")).map(_.split("=")(1)).orNull

    if (key == null || secret == null) throw new Exception("Twitter credentials not found in expected format")
    (key, secret)
  }

  /**
   * Generates a bearer token for Twitter API authentication.
   * @param apiKey The Twitter API key.
   * @param apiSecret The Twitter API secret.
   * @return The bearer token as a String.
   * @throws Exception if the token cannot be obtained.
   */
  def getBearerToken(apiKey: String, apiSecret: String): String = {
    val client = HttpClients.createDefault()
    val post = new HttpPost("https://api.twitter.com/oauth2/token")
    val credentials = Base64.getEncoder.encodeToString(s"$apiKey:$apiSecret".getBytes(StandardCharsets.UTF_8))

    post.addHeader("Authorization", s"Basic $credentials")
    post.addHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
    post.setEntity(new UrlEncodedFormEntity(new ArrayList[BasicNameValuePair]() {
      add(new BasicNameValuePair("grant_type", "client_credentials"))
    }))

    val response = client.execute(post)
    val responseString = EntityUtils.toString(response.getEntity, "UTF-8")
    scala.util.parsing.json.JSON.parseFull(responseString) match {
      case Some(map: Map[String, Any]) => map("access_token").toString
      case _ => throw new Exception("Failed to obtain bearer token")
    }
  }

  /**
   * Main method to set up Spark Streaming and process tweets.
   * @param args Command-line arguments (not used).
   */
  def main(args: Array[String]): Unit = {
    setupLogging()
    val (apiKey, apiSecret) = getTwitterCredentials()
    val bearerToken = getBearerToken(apiKey, apiSecret)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SaveTweets")
    val ssc = new StreamingContext(conf, Seconds(1))
    val tweets = ssc.receiverStream(new CustomTwitterReceiver(bearerToken))

    var totalTweets: Long = 0
    tweets.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty()) {
        val singlePartitionRDD = rdd.coalesce(1).cache()
        singlePartitionRDD.saveAsTextFile(s"checkpoint/Tweets_${time.milliseconds}")
        totalTweets += singlePartitionRDD.count()
        println(s"Tweet count: $totalTweets")
        if (totalTweets > 1000) System.exit(0)
      }
    })

    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * CustomTwitterReceiver
 * A custom Spark Streaming receiver to fetch tweets from Twitter's API.
 * @param bearerToken The bearer token for Twitter API authentication.
 */
class CustomTwitterReceiver(bearerToken: String)
  extends org.apache.spark.streaming.receiver.Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  /**
   * Starts the receiver by launching a thread to fetch tweets.
   */
  def onStart(): Unit = new Thread(() => receive()).start()

  /**
   * Stops the receiver. (No additional cleanup required.)
   */
  def onStop(): Unit = {}

  /**
   * Fetches tweets in a loop and stores them in Spark's memory.
   */
  private def receive(): Unit = {
    try {
      while (!isStopped()) {
        fetchTweets(bearerToken).foreach(store)
        Thread.sleep(10)
      }
    } catch {
      case e: Exception =>
        reportError("Error receiving data", e)
        restart("Restarting", e)
    }
  }

  /**
   * Fetches tweets from Twitter's API.
   * @param bearerToken The bearer token for authentication.
   * @return A sequence of tweet texts.
   */
  private def fetchTweets(bearerToken: String): Seq[String] = {
    val client = HttpClients.createDefault()
    val request = new HttpGet("https://api.twitter.com/2/tweets/search/recent?query=scala&max_results=10")
    request.addHeader("Authorization", s"Bearer $bearerToken")

    val response = client.execute(request)
    val responseString = EntityUtils.toString(response.getEntity, "UTF-8")
    parseJson(responseString)
  }

  /**
   * Parses JSON response from Twitter API to extract tweet texts.
   * @param jsonStr The JSON string from the API response.
   * @return A sequence of tweet texts.
   */
  private def parseJson(jsonStr: String): Seq[String] = {
    val result = new ArrayBuffer[String]()
    scala.util.parsing.json.JSON.parseFull(jsonStr) match {
      case Some(map: Map[String, Any]) =>
        map.get("data").collect {
          case dataList: List[Map[String, Any]] =>
            dataList.foreach(tweet => result += tweet.getOrElse("text", "").toString)
        }
      case _ => // Invalid JSON
    }
    result.toSeq
  }
}