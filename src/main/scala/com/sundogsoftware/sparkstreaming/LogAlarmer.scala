package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverError, StreamingListenerReceiverStopped, StreamingListenerReceiverStarted}

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import java.util.concurrent.atomic.AtomicLong

object LogAlarmer {

  def main(args: Array[String]) {

    // Parse command-line arguments with defaults
    val windowLength = if (args.length > 0) args(0).toInt else 300 // seconds
    val host = if (args.length > 1) args(1) else "127.0.0.1"
    val port = if (args.length > 2) args(2).toInt else 9999
    val maxErrorRatio = if (args.length > 3) args(3).toDouble else 0.5
    val noDataTimeout = if (args.length > 4) args(4).toInt else 600 // seconds

    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))
    setupLogging()
    val pattern = apacheLogPattern()

    // Add listener to monitor receiver errors and restarts
    ssc.addStreamingListener(new StreamingListener {
      override def onReceiverError(error: StreamingListenerReceiverError): Unit = {
        println(s"ALERT: Receiver error detected: ${error.receiverInfo.lastErrorMessage}")
      }
      override def onReceiverStopped(stopped: StreamingListenerReceiverStopped): Unit = {
        println(s"ALERT: Receiver stopped: ${stopped.receiverInfo.lastErrorMessage}")
      }
      override def onReceiverStarted(started: StreamingListenerReceiverStarted): Unit = {
        println(s"INFO: Receiver started: ${started.receiverInfo.name}")
      }
    })

    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)

    // Track last alarm and last data received times
    val lastAlarmTime = new AtomicLong(0L)
    val lastDataTime = new AtomicLong(System.currentTimeMillis())

    // Update lastDataTime whenever data is received
    lines.foreachRDD(rdd => if (!rdd.isEmpty()) lastDataTime.set(System.currentTimeMillis()))

    val statuses = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) matcher.group(6) else "[error]"
    })

    val successFailure = statuses.map(x => {
      val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) "Success"
      else if (statusCode >= 500 && statusCode < 600) "Failure"
      else "Other"
    })

    val statusCounts = successFailure.countByValueAndWindow(Seconds(windowLength), Seconds(1))

    statusCounts.foreachRDD((rdd, time) => {
      var totalSuccess: Long = 0
      var totalError: Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") totalSuccess += count
          if (result == "Failure") totalError += count
        }
      }

      println(s"Total success: $totalSuccess Total failure: $totalError")

      val now = System.currentTimeMillis()
      val minData = 100

      // Alarm if error ratio is too high, but only once every 30 minutes
      if (totalError + totalSuccess > minData) {
        val ratio: Double = util.Try(totalError.toDouble / totalSuccess.toDouble) getOrElse 1.0
        println("ratio: " + ratio)
        if (ratio > maxErrorRatio) {
          if (now - lastAlarmTime.get() > 30 * 60 * 1000) { // 30 minutes
            println("Wake somebody up! Something is horribly wrong.")
            lastAlarmTime.set(now)
          } else {
            println("Alarm suppressed to avoid spamming.")
          }
        } else {
          println("All systems go.")
        }
      }
    })

    // Alarm if no data received within noDataTimeout seconds
    lines.foreachRDD(rdd => {
      val now = System.currentTimeMillis()
      if (rdd.isEmpty() && now - lastDataTime.get() > noDataTimeout * 1000) {
        if (now - lastAlarmTime.get() > 30 * 60 * 1000) {
          println(s"ALERT: No data received in the last $noDataTimeout seconds!")
          lastAlarmTime.set(now)
        }
      }
    })

    ssc.checkpoint("checkpoint/logAlarmer/")
    ssc.start()
    ssc.awaitTermination()
  }
}