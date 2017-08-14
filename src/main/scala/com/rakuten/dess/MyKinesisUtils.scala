package com.rakuten.dess

// AWS, AWS Kinesis library
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel

// Spark
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// Jackson JSON

// Java

// Utils
import com.typesafe.config.ConfigFactory

object MyKinesisUtils {
//  val credential = ConfigFactory.load("credential.conf")
  val credential = ConfigFactory.load("credential.conf")
  val config = ConfigFactory.load("kinesis.conf")

  /* ------------------------ Create streams ------------------------ */
  /* Create stream with user-input parameters: mainly for testing */
  def createStreamWithName(ssc: StreamingContext, streamName: String, appName: String, checkpointInterval: Int, initPosition: InitialPositionInStream): DStream[Array[Byte]] = {
    KinesisUtils.createStream(ssc, appName, streamName, "kinesis.us-east-1.amazonaws.com", "us-east-1", initPosition,
      Seconds(checkpointInterval), StorageLevel.MEMORY_AND_DISK)
  }

  /* Create stream with configuration from: kinesis.conf, default 1 receiver */
  def createStream(ssc: StreamingContext, appTag: String, streamTag: String, latest: Boolean = true): DStream[Array[Byte]] = {
    val initialType = if (latest) InitialPositionInStream.LATEST else InitialPositionInStream.TRIM_HORIZON
    KinesisUtils.createStream(ssc, config.getString(appTag+"."+streamTag+".app-name"),
      config.getString(appTag+"."+streamTag+".stream-name"), config.getString(appTag+".endpoint-url"),
      config.getString(appTag+".region-name"), initialType,
      Seconds(config.getInt(appTag+".checkpoint-interval-second")), StorageLevel.MEMORY_AND_DISK, credential.getString("aws.access-key-id"), credential.getString("aws.secret-access-key"))
  }

  /* Create stream with configuration from: kinesis.conf, with abritrary number of receivers */
  def createStreams(ssc: StreamingContext, appTag: String, streamTag: String, numberOfStreams: Int, latest: Boolean = true): DStream[Array[Byte]] = {
    if (numberOfStreams > 1) {
      ssc.union((0 until numberOfStreams).map { i =>
        createStream(ssc, appTag, streamTag, latest)
      })
    }
    else {
      createStream(ssc, appTag, streamTag, latest)
    }
  }

}
class MyKinesisUtils