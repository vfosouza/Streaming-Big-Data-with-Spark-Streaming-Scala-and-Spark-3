package com.sundogsoftware.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Matcher

import Utilities._

/** Maintains top referrers and user agents visited over a 5 minute window, from a stream
 *  of Apache access logs on port 9999.
 */
object LogParser {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Cache the DStream to avoid recomputing for both referrer and user agent analysis
    val cachedLines = lines.cache()

    // Extract the referrer field from each log line (group 8)
    val referrers = cachedLines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) matcher.group(8) else "[error]"
    })

    // Extract the user agent field from each log line (group 9)
    val userAgents = cachedLines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) matcher.group(9) else "[error]"
    })

    // Reduce by referrer over a 5-minute window sliding every second
    val referrerCounts = referrers.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Reduce by user agent over a 5-minute window sliding every second
    val userAgentCounts = userAgents.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results for referrers
    val sortedReferrers = referrerCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedReferrers.print(5)

    // Sort and print the results for user agents
    val sortedUserAgents = userAgentCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedUserAgents.print(5)

    // Kick it off
    ssc.checkpoint("checkpoint/logParser/")
    ssc.start()
    ssc.awaitTermination()
  }
}