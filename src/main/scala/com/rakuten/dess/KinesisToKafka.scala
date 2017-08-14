package com.rakuten.dess

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.fasterxml.jackson.databind.JsonNode
import com.rakuten.dess.config.Config
import com.rakuten.dess.utils.KafkaProducerUtils.KafkaSink
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Swapnil on 7/6/17.
  */
object KinesisToKafka {
  // Logger
  val logger = Logger.getLogger(classOf[KinesisToKafka]);

  // MAIN METHOD
  def main(args: Array[String]) {
    // Read cmd arguments
    val config = Config.load(args(0), classOf[Config])
    executeKinesisStream(config)
  }

  def executeKinesisStream(config: Config) {
    // Spark & Stream Context
    val sc = new SparkContext(Config.sparkConfig(config))
    val ssc = new StreamingContext(sc, Seconds(config.getSparkBatchIntervalSeconds))
    val kinesisEndpointUrl = config.getKinesisEndpointUrl

    val kinesisEventsStream = KinesisConsumerUtils.createStream[JsonNode](2, ssc, config.getKinesisDynamoDbName, config.getKinesisStreamName, kinesisEndpointUrl,RegionUtils.getRegionByEndpoint(kinesisEndpointUrl).getName(),
      InitialPositionInStream.LATEST, Seconds(60))
    val kafkaSink = KafkaSink[String, JsonNode](config.getKafkaProducerConfig())

    kinesisEventsStream.foreachRDD( rdd => {
      rdd.foreachPartition( partition => {
        partition.foreach({ case(x,y) =>
          kafkaSink.send(x,y)
        })
      })
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

class KinesisToKafka