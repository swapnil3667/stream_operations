package com.rakuten.dess

/**
  * Created by Swapnil on 8/6/17.
  */
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory

object KinesisConsumerUtils {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  val credential = ConfigFactory.load("credential.conf")

  def createStream[V: ClassTag](numStreams: Int, ssc: StreamingContext, appName: String, streamName: String, endpointUrl: String, regionName:  String, initialPositionInStream: InitialPositionInStream, kinesisCheckpointInterval: Duration, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
    val massageHandler = (record: Record) => (record.getPartitionKey, mapper.readValue(record.getData.array(), implicitly[reflect.ClassTag[V]].runtimeClass.asInstanceOf[Class[V]]))
    // Create the Kinesis DStreams
    ssc.union((0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName,
        initialPositionInStream, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK, massageHandler, credential.getString("aws.access-key-id"), credential.getString("aws.secret-access-key"))
    })
  }
}
class KinesisConsumerUtils