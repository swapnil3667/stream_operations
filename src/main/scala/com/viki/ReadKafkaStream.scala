package com.viki

/**
  * Created by Swapnil on 9/6/17.
  */

// Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// Utils
import com.typesafe.config.ConfigFactory
import org.apache.log4j._
import com.viki.util.TwitterUtilities

// For local testing
import scala.collection.mutable.Queue

import org.apache.spark.streaming.kafka._

//import org.apache.spark.streaming.kafka.KafkaUtils

object ReadKafkaStream {
  // Logger
  val logger = Logger.getLogger("ReadKafkaStream");

  // MAIN METHOD
  def main(args: Array[String]) {
    // Read cmd arguments

    TwitterUtilities.setUpTwitter()
    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "zookeeper.connection.timeout.ms" -> "10000",
      "group.id" -> "myGroup"
    )

    val topics = Map(
      "comeone" -> 1
    )
    val conf = new SparkConf().setAppName("PrintTweets").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val messages = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup",  topics)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    //   val tweets = TwitterUtils.createStream(ssc, None)
    messages.foreachRDD( message =>
      message.foreach{x =>
        val jsonNode = x._2
        println(jsonNode)
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }

}

