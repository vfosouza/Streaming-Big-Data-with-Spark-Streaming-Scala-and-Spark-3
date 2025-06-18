
package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** An example of using a State object to keep persistent state information across a stream. 
 *  In this case, we'll keep track of clickstreams on sessions tied together by IP addresses.
 */
object Sessionizer {
  
  /** This "case class" lets us quickly define a complex type that contains a session length and a list of URL's visited,
   *  which makes up the session data state that we want to preserve across a given session. The "case class" automatically
   *  creates the constructor and accessors we want to use.
   */
  case class SessionData(val sessionLength: Long, var clickstream:List[String], var successCodes: Long = 0, var errorCodes: Long = 0);
  
  /** This function gets called as new data is streamed in, and maintains state across whatever key you define. In this case,
   *  it expects to get an IP address as the key, a String as a URL (wrapped in an Option to handle exceptions), and 
   *  maintains state defined by our SessionData class defined above. Its output is new key, value pairs of IP address
   *  and the updated SessionData that takes this new line into account.
   */
  def trackStateFunc(batchTime: Time, ip: String, urlAndStatus: Option[(String, String)],
                     state: State[SessionData]): Option[(String, SessionData)] = {
    // Extract the previous state passed in
    val previousState = state.getOption.getOrElse(SessionData(0, List(), 0, 0))

    // Extract URL and status code
    val (url, statusCode) = urlAndStatus.getOrElse(("empty", "0"))

    // Determine if status code indicates success (2xx, 3xx) or error (4xx, 5xx)
    val isSuccess = try {
      val code = statusCode.toInt
      code >= 200 && code < 400
    } catch {
      case _: Exception => false
    }

    // Create a new state with updated counters
    val newState = SessionData(
      previousState.sessionLength + 1L,
      (previousState.clickstream :+ url).take(10),
      previousState.successCodes + (if (isSuccess) 1L else 0L),
      previousState.errorCodes + (if (!isSuccess) 1L else 0L)
    )

    // Update our state
    state.update(newState)

    // Return a new key/value result
    Some((ip, newState))
  }
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "Sessionizer", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // We'll define our state using our trackStateFunc function above, and also specify a 
    // session timeout value of 30 minutes.
    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))
                         
    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the (ip, url) we want from each log line
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val statusCode = matcher.group(6) // Extract status code
        val requestFields = request.toString().split(" ")
        val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
        (ip, (url, statusCode)) // Return IP, URL, and status code as tuple
      } else {
        ("error", ("error", "0"))
      }
    })
    
    // Now we will process this data through our StateSpec to update the stateful session data
    // Note that our incoming RDD contains key/value pairs of ip/URL, and that what our
    // trackStateFunc above expects as input.
    val requestsWithState = requests.mapWithState(stateSpec)
    
    // And we'll take a snapshot of the current state so we can look at it.
    val stateSnapshotStream = requestsWithState.stateSnapshots()  
 
    // Process each RDD from each batch as it comes in
    stateSnapshotStream.foreachRDD((rdd, time) => {

      // We'll expose the state data as SparkSQL, but you could update some external DB
      // in the real world.
      
      val spark = SparkSession
         .builder()
         .appName("Sessionizer")
         .getOrCreate()
         
      import spark.implicits._
      
      // Slightly different syntax here from our earlier SparkSQL example. toDF can take a list
      // of column names, and if the number of columns matches what's in your RDD, it just works
      // without having to use an intermediate case class to define your records.
      // Our RDD contains key/value pairs of IP address to SessionData objects (the output from
      // trackStateFunc), so we first split it into 3 columns using map().
      val requestsDataFrame = rdd.map(x => (x._1,
                                            x._2.sessionLength,
                                            x._2.clickstream,
                                            x._2.successCodes,
                                            x._2.errorCodes))
        .toDF("ip", "sessionLength", "clickstream", "successCodes", "errorCodes")

      // Create a SQL table from this DataFrame
      requestsDataFrame.createOrReplaceTempView("sessionData")

      // Dump out the results - you can do any SQL you want here.
      val sessionsDataFrame =
        spark.sqlContext.sql("select * from sessionData")
      println(s"========= $time =========")
      sessionsDataFrame.show()

    })
    
    // Kick it off
    ssc.checkpoint("checkpoint/Sessionizer/")
    ssc.start()
    ssc.awaitTermination()
  }
}



