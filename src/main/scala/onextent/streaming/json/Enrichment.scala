package onextent.streaming.json


import java.util.Properties

import com.github.benfradet.spark.kafka010.writer._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Enrichment extends Serializable with LazyLogging {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load().getConfig("main")

    val sparkConfig = new SparkConf()
      .set("spark.cores.max", "2")
      .setIfMissing("spark.master", "local[*]")

    val ssc = new StreamingContext(new SparkContext(sparkConfig), Seconds(config.getString("kafka.batchDuration").toInt))

    val producerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", config.getString("kafka.brokerList"))
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      //p.setProperty("value.serializer", classOf[BytesSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getString("kafka.brokerList"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getString("kafka.consumerGroup"),
      "auto.offset.reset" -> config.getString("kafka.offsetReset"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(config.getString("kafka.inputTopic"))

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    /*
    stream.map(record => (record.key, record.value)).foreachRDD(rdd => rdd.foreach(o => {
      println(s"key ${o._1} val: ${o._2}")
      //val sendEvent = new EventData(o._2.getBytes("UTF8"))
      //EhPublisher.ehClient.send(sendEvent)
    }))
    */

    stream.map(record => {
      (record.key, record.value)
    })
   .writeToKafka[String, String](
      producerConfig,
      d => new ProducerRecord[String, String](config.getString("kafka.outputTopic"), new String(d._2.getBytes("UTF8")))
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
