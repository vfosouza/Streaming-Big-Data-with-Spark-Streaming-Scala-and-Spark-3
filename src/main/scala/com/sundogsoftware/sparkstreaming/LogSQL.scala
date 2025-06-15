
package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
 *  Apache log data extracted from a stream on port 9999.
 */
object LogSQL {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setAppName("LogSQL").setMaster("local[*]").set("spark.sql.warehouse.dir", "file:///tmp")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })
 
    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {
      // Skip empty RDDs
      if (!rdd.isEmpty()) {
        val spark = SparkSession
          .builder()
          .appName("LogSQL")
          .getOrCreate()

        import spark.implicits._

        // Convert RDD to DataFrame
        val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

        // Create common base path and unique output paths
        val timestamp = time.milliseconds
        val basePath = "output/LogSQL"
        val rawDataPath = s"$basePath/requests_$timestamp"
        val aggregatePath = s"$basePath/agent_counts_$timestamp"

        // Write the raw data to JSON files
        println(s"========= $time =========")
        println(s"Writing raw data to $rawDataPath")
        requestsDataFrame.write.json(rawDataPath)

        // Compute and write aggregated data
        val agentCountsDF = requestsDataFrame.groupBy("agent").count()
        println(s"Writing aggregated data to $aggregatePath")
        agentCountsDF.write.json(aggregatePath)
      }
    })
    
    // Kick it off
    ssc.checkpoint("checkpoint/LogSQL/")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Record(url: String, status: Int, agent: String)


