import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaExample {
  def main(args: Array[String]): Unit = {
    // Set up the Spark configuration
    val sparkConf = new SparkConf().setAppName("KafkaExample").setMaster("local[*]")

    // Create StreamingContext with a 2 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Define Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-example",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // List of topics you want to listen for from Kafka
    val topics = Array("testLogs") // Replace with your actual topic name

    // Create direct Kafka stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Process the stream
    val lines = stream.map(record => record.value)

    // Count words
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

    // Print the results
    wordCounts.print()

    // Start the stream processing
    ssc.start()
    ssc.awaitTermination()
  }
}