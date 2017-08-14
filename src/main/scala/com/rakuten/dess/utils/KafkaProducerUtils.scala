package com.rakuten.dess.utils

/**
  * Created by Swapnil on 10/8/17.
  */


import com.rakuten.dess.config.KafkaProducerConfig

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord;

object KafkaProducerUtils {

  class MyKafkaProducer[K, V](val conf: KafkaProducerConfig) extends KafkaProducer[K, V](conf.toProperties()) {
    private val topicName = conf.getTopicName()

    def send(key: K, value: V) {
      send(new ProducerRecord[K, V](topicName, key, value))
    }
  }

  class KafkaSink[K, V](createProducer: () => MyKafkaProducer[K, V]) extends Serializable {

    lazy val producer = createProducer()

    def send(key: K, value: V) = producer.send(key, value)

  }

  object KafkaSink {
    def apply[K, V](config: KafkaProducerConfig): KafkaSink[K, V] = {
      val f = () => {
        val producer = new MyKafkaProducer[K, V](config)

        sys.addShutdownHook {
          producer.close()
        }

        producer
      }
      new KafkaSink[K, V](f)
    }
  }
}