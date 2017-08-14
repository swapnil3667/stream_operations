package com.rakuten.dess

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.rabbitmq.client.ConnectionFactory
import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.{HttpDelete, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

/**
  * Created by Swapnil on 7/6/17.
  */

object MonitorVikilitics {
  // App Config File
  val appConfig = ConfigFactory.load("monitor_vikilitics.conf")
  val credential = ConfigFactory.load("credential.conf")
  val signalFXConfig = ConfigFactory.load("signalFX.conf")
  val rabbitMqConfig = ConfigFactory.load("rabbitmq.conf")
  // Logger
  val logger = Logger.getLogger(classOf[MonitorVikilitics])

  val acceptedEvents = appConfig.getStringList("spark.acceptedEvents")
  val acceptedAppId = appConfig.getStringList("spark.acceptedAppId")
  val clickAppId = appConfig.getStringList("spark.clickAppId")
  val clickAcceptedWhat = appConfig.getStringList("spark.clickAcceptedWhat")
  val clickAcceptedPage = appConfig.getStringList("spark.clickAcceptedPage")
  val acceptedRabbitMqEvents = appConfig.getStringList("spark.acceptedRabbitMqEvents")

  case class FilterEvents(eventType: String, appId: String, appVer: String)
  case class FilterRabbitMqEvents(eventType: String, appId: String, appVer: String, userId: String, videoId: String, tMs: String, watchMarker: String, videoWatchTime: String, completedContentPercent: String)

  var sc: SparkContext = null

  val prometheusUrl = appConfig.getString("spark.prometheusUrl")
  import com.rabbitmq.client.Channel
  var rabbitMqChannel: Channel = null

  def getOrCreateSparkContext(): SparkContext = {
    if (sc!= null) sc
    else {
      val conf = new SparkConf().setAppName(appConfig.getString("spark.app-name")).setMaster("local[*]")
      sc = new SparkContext(conf)
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", credential.getString("aws.access-key-id"))
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", credential.getString("aws.secret-access-key"))
      sc
    }
  }

  def getOrCreateRabbitMqChannel() {
    if (rabbitMqChannel != null) rabbitMqChannel
    else {
      val factory = new ConnectionFactory()
      val queue = "watcheventsnew"
      val exchangename = rabbitMqConfig.getString("rabbitmq.exchangename")
    //  factory.setUsername(rabbitMqConfig.getString("rabbitmq.username"))
    //  factory.setPassword(rabbitMqConfig.getString("rabbitmq.password"))
      factory.setHost(rabbitMqConfig.getString("rabbitmq.host"))
    //  factory.setPort(rabbitMqConfig.getInt("rabbitmq.port"))
      val connection = factory.newConnection()
      rabbitMqChannel = connection.createChannel()
    //  rabbitMqChannel.queueDeclare(rabbitMqConfig.getString("rabbitmq.queuename"), true, false, false, null)
      rabbitMqChannel.exchangeDeclare(exchangename, "topic", true, false, false, null)
      rabbitMqChannel.queueDeclare(queue, true, false, false, null)
      rabbitMqChannel.queueBind(queue, exchangename, "events.watchtime")
      rabbitMqChannel
    }
  }

  // MAIN METHOD
  def main(args: Array[String]) {
    // Read cmd arguments
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
    executeKinesisStream(batchIntervalSeconds, initPosition)
  }


  def executeKinesisStream(batchIntervalSeconds: Int, initPosition: InitialPositionInStream) {
    // Spark Streaming batch interval
    val batchInterval = Seconds(batchIntervalSeconds)

    // Spark & Stream Context

    val ssc = new StreamingContext(getOrCreateSparkContext, batchInterval)
    //  ssc.checkpoint(appConfig.getString("spark.checkpoint-dir"))

    // Create Events stream
    val kinesisEventsStream = MyKinesisUtils.createStreams(ssc, "hydrate-events", "input", appConfig.getInt("spark.number-event-streams"))

    val eventsStringStream = kinesisEventsStream.map(byteArray => new String(byteArray)).cache()
    val eventStream = filterEventStream(eventsStringStream)
    val rabbitMqEventStream = filterRabbitMqEventSream(eventsStringStream)


    getOrCreateRabbitMqChannel()

    rabbitMqEventStream.foreachRDD{
      rdd => {
        rdd.foreachPartition ( partition => {
          partition.foreach ({ p =>
            val message = Json(DefaultFormats).write(p)
            rabbitMqChannel.basicPublish(rabbitMqConfig.getString("rabbitmq.exchangename"), "events.watchtime", null, message.getBytes())
          })
        })
      }
    }


    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    eventStream.foreachRDD( rdd => {

      val logCount = rdd.count()

      val eventDF = rdd.toDF()

      val resultDF = eventDF.select("eventType","appId","appVer").groupBy("eventType","appId","appVer")
        .agg(count("*").alias("eventTypeCount")).sort($"eventType",$"eventTypeCount".desc)

 //     val result = filterMetrics(resultDF.collect.toList).map{
 //       f => SignalFX.generateMetric(signalFXConfig.getString("signalFX.metricsName"), Map(("event", f.getString(0)),
 //         ("appId", f.getString(1)), ("version", f.getString(2))), f.getLong(3))
 //     }
      //SignalFX.post(result.toList)

      //      val resultPrometheus = resultDF.collect.map{
      //        f  => s"""viki_event{event="${f(0)}", version="${f(2)}", appId="${f(1)}"} ${f(3)}"""
      //      }
      //      sendToPrometheus(resultPrometheus)

    })

    // Shut down
    sys.ShutdownHookThread {
      logger.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      rabbitMqChannel.close()
      logger.info("Application stopped")
    }

    // Start
    ssc.start()
    ssc.awaitTermination()
  }

  def filterEventStream(inputStringStream: DStream[String])= {

    inputStringStream
      .mapPartitions(lines => {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        lines.flatMap(line => {
          val jsonNode = mapper.readValue(line, classOf[JsonNode])
          val eventType = getString(jsonNode.get("event")).getOrElse(null)
          val appId = getString(jsonNode.get("app_id")).getOrElse(null)
          val appVer = getString(jsonNode.get("app_ver")).getOrElse(null)
          val what = getString(jsonNode.get("what")).getOrElse(null)
          val page = getString(jsonNode.get("page")).getOrElse(null)
          if (((acceptedEvents contains(eventType)) && (acceptedAppId contains(appId)))
            || ((eventType ==  "click") && (clickAppId contains appId) && (clickAcceptedWhat contains what) && (clickAcceptedPage contains page))) {
            Array(FilterEvents(eventType, appId, appVer))
          } else {
            None
          }
        })
      })
  }

  def filterRabbitMqEventSream(inputStringStream: DStream[String]) = {
    inputStringStream
      .mapPartitions(lines => {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        lines.flatMap(line => {
          val jsonNode = mapper.readValue(line, classOf[JsonNode])
          val eventType = getString(jsonNode.get("event")).getOrElse(null)
          val appId = getString(jsonNode.get("app_id")).getOrElse(null)
          val appVer = getString(jsonNode.get("app_ver")).getOrElse(null)
          val userId = getString(jsonNode.get("user_id")).getOrElse(null)
          val tMs = getString(jsonNode.get("t_ms")).getOrElse(null)
          val videoId = getString(jsonNode.get("video_id")).getOrElse(null)
          val watchMarker = getString(jsonNode.get("watch_marker")).getOrElse(null)
          val videoWatchTime = getString(jsonNode.get("video_watch_time")).getOrElse(null)
          val completedContentPercent = getString(jsonNode.get("completed_content_percent")).getOrElse(null)
          val page = getString(jsonNode.get("page")).getOrElse(null)
          if (acceptedRabbitMqEvents contains(eventType)) {
            Array(FilterRabbitMqEvents(eventType, appId, appVer,userId,videoId,tMs,watchMarker,videoWatchTime,completedContentPercent))
          } else {
            None
          }
        })
      })
  }

  def sendToPrometheus(result: Array[String]) = {
    val params = "\n" +result.mkString("\n") + "\n"

    val delete = new HttpDelete(prometheusUrl)
    val deleteResponse = (new DefaultHttpClient).execute(delete)

    val post = new HttpPost(prometheusUrl)

    // set the Content-type
    post.setHeader("Content-type", "application/x-www-form-urlencoded")

    // add the JSON as a StringEntity
    post.setEntity(new StringEntity(params))

    // send the post request
    val response = (new DefaultHttpClient).execute(post)
    response.getAllHeaders.foreach(arg => logger.info(arg))
  }

  def getString(jsValue: JsonNode): Option[String] = {
    if (jsValue == null) None
    else {
      val v = jsValue.textValue()
      if (v == null) None
      else if (v.trim == "") None
      else if (v.trim == "null") None
      else Some(v)
    }
  }

  def filterMetrics(rawResult: List[Row] ) = {
    var rawMetrics = List[Row]()
    val eventLimit = signalFXConfig.getInt("signalFX.eventLimit")
    if (rawResult.size > eventLimit) {
      var startIndex = 0
      for (i <- 1 until rawResult.size) {
        if (rawResult(i).getString(0) != rawResult(i-1).getString(0)) {
          if (i - startIndex <= eventLimit) {
            rawMetrics ++= rawResult.slice(startIndex, i)
          } else {
            rawMetrics ++= rawResult.slice(startIndex, startIndex + eventLimit)
          }
          startIndex = i
        }
      }
      rawMetrics ++= rawResult.slice(startIndex, startIndex + eventLimit)
    } else {
      rawMetrics = rawResult
    }
    rawMetrics
  }

}

class MonitorVikilitics
