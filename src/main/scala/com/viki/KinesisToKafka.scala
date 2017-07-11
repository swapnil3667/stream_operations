package com.viki

import java.util.Properties

import com._
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import com.viki.KinesisConsumerUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.serializer.KryoSerializer
import scala.util.Random

/**
  * Created by Swapnil on 7/6/17.
  */

object KinesisToKafka {
  // App Config File
  val appConfig = ConfigFactory.load("read-vikilitics.conf")
  val credential = ConfigFactory.load("credentials.conf")
  val kafkaConfig = ConfigFactory.load("kafka.conf")
  // Logger
 // val logger = Logger.getLogger("ReadVikilitics")
  val logger = Logger.getLogger(classOf[ReadOnly]);

  case class MyRow(resource: String, action: Int)

  // MAIN METHOD
  def main(args: Array[String]) {
    // Read cmd arguments
    val streamName = appConfig.getString("spark.stream-name")
    val dynamoAppName = appConfig.getString("spark.dynamo-app-name")
    val outDir = appConfig.getString("spark.output-dir")
    var batchIntervalSeconds = appConfig.getInt("spark.batch-interval-second")
    if (args.length > 3) {
      batchIntervalSeconds = args(3).toInt
    }


    var initPosition = InitialPositionInStream.TRIM_HORIZON
    if (args.length > 4) {
      if (args(4) == "latest") {
        initPosition = InitialPositionInStream.LATEST
      }
    }
    executeKinesisStream(streamName, dynamoAppName, outDir, batchIntervalSeconds, initPosition)
  }

  def executeKinesisStream(streamName: String, appName: String, outDir: String, batchIntervalSeconds: Int, initPosition: InitialPositionInStream) {
    // Spark Streaming batch interval
    val batchInterval = Seconds(batchIntervalSeconds)

    // Spark & Stream Context
    val conf = new SparkConf().setAppName(appConfig.getString("spark.app-name")).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchInterval)
    println(credential.getString("aws.access-key-id"))
    println(credential.getString("aws.secret-access-key"))

    val kinesisEndpointUrl = "kinesis.us-east-1.amazonaws.com"


    // Setup   Kafka
    val topic = kafkaConfig.getString("kafka.topic-name")
    val brokers = kafkaConfig.getString("kafka.broker-ip")
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "KinesisToKafka")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer")



    val kinesisEventsStream = KinesisConsumerUtils.createStream[JsonNode](2, ssc, appName, streamName, "kinesis.us-east-1.amazonaws.com",RegionUtils.getRegionByEndpoint(kinesisEndpointUrl).getName(),
      InitialPositionInStream.LATEST, Seconds(60))

    kinesisEventsStream.foreachRDD( rdd => {
      val logCount = rdd.count()
      logger.info("\n--------records in rdd: " + logCount + ", partitions:" + rdd.getNumPartitions)

      rdd.foreachPartition ( partition => {

        val producer = new KafkaProducer[String, JsonNode](props)

        var count =10
        partition.take(2).foreach(logger.info(_))
        partition.foreach ({ case(x,y) =>
          if (count > 0) {
            logger.info(y.toString)
            count = count-1
          }

          producer.send(new ProducerRecord[String, JsonNode](topic, y))
          //   sendToStreamlyzerWithPlay(p)
        })
      })
     //   producer.send(new ProducerRecord[String, MyRow](topic,MyRow(resource = y.toString,action = 10))
    })


    // Shut down
    sys.ShutdownHookThread {
      logger.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      logger.info("Application stopped")
    }

    // Start
    ssc.start()
    ssc.awaitTermination()
  }
}

class ReadOnly