package onextent.streaming.json

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Enrichment extends Serializable with LazyLogging {

  def main(args: Array[String]): Unit = {

    logger.info("starting...")

    val config = ConfigFactory.load().getConfig("main")
    val topic = config.getString("kafka.topic")

    val batchDuration = config.getString("eventhubs.batchDuration").toInt

    val sparkConfig = new SparkConf().set("spark.cores.max", "3").set("spark.cores.min", "3")
    val ssc = new StreamingContext(new SparkContext(sparkConfig), Seconds(batchDuration))


    val producerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", config.getString("kafka.brokerList"))
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }

    //ehSteam.writeToKafka[String, Array[Byte]](
    //  producerConfig,
    //  ehData => new ProducerRecord[String, Array[Byte]](topic, ehData.getBody)
    //)
    /*
    ehSteam.writeToKafka[String, String](
      producerConfig,
      ehData => new ProducerRecord[String, String](topic, new String(ehData.getBody))
    )
    */
    ssc.start()
    ssc.awaitTermination()
  }
}

