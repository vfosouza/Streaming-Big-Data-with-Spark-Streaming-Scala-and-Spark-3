package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import org.apache.spark.rdd.RDD

import java.io.File
import scala.collection.mutable

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PopularHashtags {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    //setupTwitter()

    // Parameters for experimentation
    // Batch size: How frequently data is processed
    // Window size: The duration of the window (how much historical data to consider)
    // Slide size: How frequently the window slides forward
    val batchSize = Seconds(1)    // Try different values like 2, 5, 10 seconds
    val windowSize = Seconds(10) // Try different values like 60, 120, 600 seconds
    val slideSize = Seconds(10)    // Try different values like 5, 10, 30 seconds

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val conf = new SparkConf().setAppName("PopularHashtags").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, slideSize)

    // Create a queue of RDDs to stream from
    val rddQueue = new mutable.Queue[RDD[String]]()

    // Find all tweet directories matching the pattern
    val rootDir = new File("checkpoint")

    if (!rootDir.exists()) {
      println(s"Directory ${rootDir.getAbsolutePath} does not exist")
      System.exit(1)
    }

    val tweetDirs = rootDir.listFiles()
      .filter(_.isDirectory)
      .filter(_.getName.startsWith("Tweets_"))
      .sortBy(_.getName)

    if (tweetDirs.isEmpty) {
      println(s"No directories matching the pattern 'Tweets_*' found in ${rootDir.getAbsolutePath}")
      System.exit(1)
    }

    println(s"Found ${tweetDirs.length} tweet directories:")
    tweetDirs.foreach(dir => println(s" - ${dir.getName}"))

    // Collect all tweet files from all directories - looking for part-* files instead of .txt files
    val tweetFiles = tweetDirs.flatMap { dir =>
      val files = dir.listFiles()
      if (files == null) {
        println(s"Warning: Cannot access directory ${dir.getPath}")
        Array.empty[File]
      } else {
        // Looking for files that start with "part-" instead of ending with ".txt"
        val partFiles = files.filter(_.isFile).filter(_.getName.startsWith("part-"))
        println(s"Found ${partFiles.length} tweet files in ${dir.getName}")
        partFiles
      }
    }.sortBy(f => (f.getParentFile.getName, f.getName))

    if (tweetFiles.isEmpty) {
      println("No tweet files found in any directory")
      System.exit(1)
    }

    println(s"Found ${tweetFiles.length} tweet files in total")

    // Load each file into an RDD and add to the queue
    tweetFiles.foreach { file =>
      println(s"Loading tweets from ${file.getPath}")
      val rdd = ssc.sparkContext.textFile(file.getPath)
      val count = rdd.count()
      println(s" - File contains $count tweets")
      if (count > 0) {
        rddQueue += rdd
      }
    }

    // Create a DStream from the queue
    val tweets = ssc.queueStream(rddQueue, oneAtATime = true)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status)

    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    // Now count them up over the window sliding every specified interval
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, windowSize, slideSize)

    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    // Print the top 10
    sortedResults.print

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("checkpoint/hashtags/")
    ssc.start()
    ssc.awaitTermination()
  }
}