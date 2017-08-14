package com.rakuten.dess.config;

/**
 * Created by Swapnil on 11/8/17.
 */
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaProducerConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> map = new HashMap<String, String>();

    private String topicName;

    public KafkaProducerConfig() {}

    public KafkaProducerConfig(String topicName) {
        this.topicName = topicName;
    }

    public KafkaProducerConfig(String topicName, KafkaProducerConfig other) {
        this.topicName = topicName;
        this.map.putAll(other.map);
    }
    public KafkaProducerConfig(String topicName, String keySerializerClass, String valueSerializerClass, KafkaProducerConfig other) {
        this.topicName = topicName;
        this.setKeySerializer(keySerializerClass);
        this.setValueSerializer(valueSerializerClass);
        this.map.putAll(other.map);
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
    public String getTopicName() {
        return topicName;
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.putAll(this.map);
        return props;
    }

    public Map<String,String> getParams() {
        return this.map;
    }

    public void setParams(Map<String,String> params) {
        this.map.putAll(params);
    }

    //Kafka Producer original configs

    public void setBootstrapServers(String bootstrapServers) {
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public String getBootstrapServers() {
        return map.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    public void setMetadataMaxAgeMs(String metadataMaxAgeMs) {
        map.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs);
    }

    public String getMetadataMaxAgeMs() {
        return map.get(ProducerConfig.METADATA_MAX_AGE_CONFIG);
    }

    public void setBatchSize(String batchSize) {
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    }

    public String getBatchSize() {
        return map.get(ProducerConfig.BATCH_SIZE_CONFIG);
    }

    public void setAcks(String acks) {
        map.put(ProducerConfig.ACKS_CONFIG, acks);
    }

    public String getAcks() {
        return map.get(ProducerConfig.ACKS_CONFIG);
    }

    public void setLingerMs(String lingerMs) {
        map.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    }

    public String getLingerMs() {
        return map.get(ProducerConfig.LINGER_MS_CONFIG);
    }

    public void setClientId(String clientId) {
        map.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    }

    public String getClientId() {
        return map.get(ProducerConfig.CLIENT_ID_CONFIG);
    }

    public void setSendBufferBytes(String sendBufferBytes) {
        map.put(ProducerConfig.SEND_BUFFER_CONFIG, sendBufferBytes);
    }

    public String getSendBufferBytes() {
        return map.get(ProducerConfig.SEND_BUFFER_CONFIG);
    }

    public void setReceiveBufferBytes(String receiveBufferBytes) {
        map.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
    }

    public String getReceiveBufferBytes() {
        return map.get(ProducerConfig.RECEIVE_BUFFER_CONFIG);
    }

    public void setMaxRequestSize(String maxRequestSize) {
        map.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
    }

    public String getMaxRequestSize() {
        return map.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
    }

    public void setReconnectBackoffMs(String reconnectBackoffMs) {
        map.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
    }

    public String getReconnectBackoffMs() {
        return map.get(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG);
    }

    public void setMaxBlockMs(String maxBlockMs) {
        map.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
    }

    public String getMaxBlockMs() {
        return map.get(ProducerConfig.MAX_BLOCK_MS_CONFIG);
    }

    public void setBufferMemory(String bufferMemory) {
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    }

    public String getBufferMemory() {
        return map.get(ProducerConfig.BUFFER_MEMORY_CONFIG);
    }

    public void setRetryBackoffMs(String retryBackoffMs) {
        map.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
    }

    public String getRetryBackoffMs() {
        return map.get(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    public void setCompressionType(String compressionType) {
        map.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    }

    public String getCompressionType() {
        return map.get(ProducerConfig.COMPRESSION_TYPE_CONFIG);
    }

    public void setMetricsSampleWindowMs(String metricsSampleWindowMs) {
        map.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindowMs);
    }

    public String getMetricsSampleWindowMs() {
        return map.get(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
    }

    public void setMetricsNumSamples(String metricsNumSamples) {
        map.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples);
    }

    public String getMetricsNumSamples() {
        return map.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG);
    }

    public void setMetricReporters(String metricReporters) {
        map.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, metricReporters);
    }

    public String getMetricReporters() {
        return map.get(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG);
    }

    public void setMaxInFlightRequestsPerConnection(String maxInFlightRequestsPerConnection) {
        map.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);
    }

    public String getMaxInFlightRequestsPerConnection() {
        return map.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }

    public void setRetries(String retries) {
        map.put(ProducerConfig.RETRIES_CONFIG, retries);
    }

    public String getRetries() {
        return map.get(ProducerConfig.RETRIES_CONFIG);
    }

    public void setKeySerializer(String keySerializer) {
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
    }

    public String getKeySerializer() {
        return map.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    }

    public void setValueSerializer(String valueSerializer) {
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
    }

    public String getValueSerializer() {
        return map.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    }

    public void setConnectionsMaxIdleMs(String connectionsMaxIdleMs) {
        map.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
    }

    public String getConnectionsMaxIdleMs() {
        return map.get(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG);
    }

    public void setPartitionerClass(String partitionerClass) {
        map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
    }

    public String getPartitionerClass() {
        return map.get(ProducerConfig.PARTITIONER_CLASS_CONFIG);
    }

    public void setRequestTimeoutMs(String requestTimeoutMs) {
        map.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
    }

    public String getRequestTimeoutMs() {
        return map.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    }

    public void setInterceptorClasses(String interceptorClasses) {
        map.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClasses);
    }

    public String getInterceptorClasses() {
        return map.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

}