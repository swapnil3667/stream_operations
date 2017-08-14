package com.rakuten.dess

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

import org.json4s._
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization

object SignalFXTest {
  private val url = "https://ingest.signalfx.com/v2/datapoint"
  private val token = "3RKHfZukzV0hhu6iEjsekQ" //replace into your token

  private val metricsName = "viki.event_monitor"

  private val postBatchSize = 500
  val threshold = 500

  implicit val formats = Serialization.formats(NoTypeHints)

  //def main(args: Array[String]) = {
   // val metrics = scala.collection.mutable.ArrayBuffer[Map[String, Any]]()
  //  metrics.append(generateMetric(metricsName, Map(("event", "ad_loaded"), ("appId", "100005a"), ("version", "1.0")), 30))
  //  metrics.append(generateMetric(metricsName, Map(("event", "hls_bitrate_change"), ("appId", "100004a"), ("version", "1.1")), 20))
  //  post(metrics.toList)
  //}

  def generateMetric(metricName: String, dimensions: Map[String, String], value: Long) = {
    Map("metric" -> metricName, "dimensions" -> dimensions, "value" -> value)
  }


  def post(metrics: List[Map[String, Any]]) = {
    metrics.sliding(postBatchSize, postBatchSize).foreach(
      block => {
        val data = write(Map("gauge" -> block))
        println(data)
        val post = new HttpPost(url)
        // set the Content-type
        post.setHeader("Content-type", "application/json")
        post.setHeader("X-SF-TOKEN", token)
        // add the JSON as a StringEntity
        post.setEntity(new StringEntity(data))
        // send the post request
        val response = HttpClientBuilder.create().build().execute(post)
        println(response)
      }
    )
  }
}

class SignalFXTest