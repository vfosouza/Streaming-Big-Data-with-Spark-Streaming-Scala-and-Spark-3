package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.io.File
import scala.collection.mutable
import org.apache.spark.rdd.RDD

/** Uses thread-safe counters to keep track of the longest
 *  Tweet in a stream.
 */
object LongestTweetLength {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Set up Spark context
    val conf = new SparkConf().setAppName("LongestTweetLength").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Set up Spark streaming context with 1-second batches
    val ssc = new StreamingContext(sc, Seconds(1))

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
      val rdd = sc.textFile(file.getPath)
      val count = rdd.count()
      println(s" - File contains $count tweets")
      if (count > 0) {
        rddQueue += rdd
      }
    }

    // Create a DStream from the queue
    val tweets = ssc.queueStream(rddQueue, oneAtATime = true)

    // Map to character lengths
    val lengths = tweets.map(status => status.length)

    // Thread-safe counters
    var totalTweets = new AtomicLong(0)
    var maxTweetLength = new AtomicLong(0)

    lengths.foreachRDD((rdd, time) => {
      var count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)

        // Find the maximum length in this RDD
        val maxInRdd = rdd.reduce((a, b) => Math.max(a, b))

        // Update the global maximum if needed - simpler approach
        val currentMax = maxTweetLength.get()
        if (maxInRdd > currentMax) {
          maxTweetLength.compareAndSet(currentMax, maxInRdd)
        }

        println("Total tweets: " + totalTweets.get() +
          " Longest tweet: " + maxTweetLength.get() + " characters")
      }
    })

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("checkpoint/longest/")
    ssc.start()
    ssc.awaitTermination()
  }
}