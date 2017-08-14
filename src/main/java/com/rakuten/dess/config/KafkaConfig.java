package com.rakuten.dess.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

public class KafkaConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String PROP_METADATA_BROKER_LIST = "metadata.broker.list";
	public static final String PROP_SERIALIZER_CLASS = "serializer.class";
	public static final String PROP_KEY_SERIALIZER_CLASS = "key.serializer.class";
	public static final String PROP_REQUEST_REQUIRED_ACKS = "request.required.acks";
	public static final String PROP_PRODUCER_TYPE = "producer.type";
	public static final String PROP_ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String PROP_GROUP_ID = "group.id";
	public static final String PROP_AUTO_COMMIT_ENABLE = "auto.commit.enable";
	public static final String PROP_AUTO_OFFSET_RESET = "auto.offset.reset";
	public static final String PROP_AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
	public static final String PROP_PARTITIONER_CLASS = "partitioner.class";
	public static final String PROP_MESSAGE_SEND_MAX_RETRIES = "message.send.max.retries";
	public static final String PROP_FETCH_MESSAGE_MAX_BYTES = "fetch.message.max.bytes";
	public static final String PROP_RETRY_BACKOFF_MS = "retry.backoff.ms";
	public static final String PROP_BATCH_NUM_MESSAGES = "batch.num.messages";
	public static final String PROP_QUEUE_BUFFERING_MAX_MS = "queue.buffering.max.ms";
	public static final String PROP_QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
	public static final String PROP_SEND_BUFFER_BYTES = "send.buffer.bytes";
	public static final String PROP_COMPRESSION_CODEC = "compression.codec";

	private final HashMap<String,String> map = new HashMap<String,String>();
	private Boolean enabled = false;
	private String topicName;
	private Integer numStreams;
	private Integer schemaVersion = -1;
	private String schemaRepoUrl;

	public KafkaConfig() {}

	public KafkaConfig(String topicName) {
		this.topicName = topicName;
	}

	public KafkaConfig(String topicName, KafkaConfig other) {
		this.topicName = topicName;
		this.map.putAll(other.map);
	}
	public KafkaConfig(String topicName, String serializerClass, KafkaConfig other) {
		this.topicName = topicName;
		this.setSerializerClass(serializerClass);
		this.map.putAll(other.map);
	}
	
	public KafkaConfig(Boolean enabled) {
		this.enabled = enabled;
	}
	
	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}
	public Boolean getEnabled() {
		return enabled;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	public String getTopicName() {
		return topicName;
	}

	public Integer getNumStreams() {
		return numStreams;
	}
	public void setNumStreams(Integer numStreams) {
		this.numStreams = numStreams;
	}

	public void setMetadataBrokerList(String metadataBrokerList) {
		map.put(PROP_METADATA_BROKER_LIST,metadataBrokerList);
	}
	public String getMetadataBrokerList() {
		return map.get(PROP_METADATA_BROKER_LIST);
	}
	public void clearMetadataBrokerList() {
		map.remove(PROP_METADATA_BROKER_LIST);
	}

	public String getSerializerClass() {
		return map.get(PROP_SERIALIZER_CLASS);
	}
	public void setSerializerClass(String serializerClass) {
		map.put(PROP_SERIALIZER_CLASS,serializerClass);
	}
	public void clearSerializerClass() {
		map.remove(PROP_SERIALIZER_CLASS);
	}

	public String getKeySerializerClass() {
		return this.map.containsKey(PROP_KEY_SERIALIZER_CLASS) ?
			map.get(PROP_KEY_SERIALIZER_CLASS) : map.get(PROP_SERIALIZER_CLASS);
	}
	public void setKeySerializerClass(String keySerializerClass) {
		map.put(PROP_KEY_SERIALIZER_CLASS,keySerializerClass);
	}
	public void clearKeySerializerClass() {
		map.remove(PROP_KEY_SERIALIZER_CLASS);
	}

	public String getRequestRequiredAcks() {
		return map.get(PROP_REQUEST_REQUIRED_ACKS);
	}
	public void setRequestRequiredAcks(String requestRequiredAcks) {
		map.put(PROP_REQUEST_REQUIRED_ACKS,requestRequiredAcks);
	}
	public void clearRequestRequiredAcks() {
		map.remove(PROP_REQUEST_REQUIRED_ACKS);
	}

	public String getProducerType() {
		return map.get(PROP_PRODUCER_TYPE);
	}
	public void setProducerType(String producerType) {
		map.put(PROP_PRODUCER_TYPE,producerType);
	}
	public void clearProducerType() {
		map.remove(PROP_PRODUCER_TYPE);
	}

	public String getZookeeperConnect() {
		return map.get(PROP_ZOOKEEPER_CONNECT);
	}
	public void setZookeeperConnect(String zookeeperConnect) {
		map.put(PROP_ZOOKEEPER_CONNECT,zookeeperConnect);
	}
	public void clearZookeeperConnect() {
		map.remove(PROP_ZOOKEEPER_CONNECT);
	}

	public String getGroupId() {
		return map.get(PROP_GROUP_ID);
	}
	public void setGroupId(String groupId) {
		map.put(PROP_GROUP_ID,groupId);
	}
	public void clearGroupId() {
		map.remove(PROP_GROUP_ID);
	}

	public String getAutoCommitEnable() {
		return map.get(PROP_AUTO_COMMIT_ENABLE);
	}
	public void setAutoCommitEnable(String autoCommitEnable) {
		map.put(PROP_AUTO_COMMIT_ENABLE,autoCommitEnable);
	}
	public void clearAutoCommitEnable() {
		map.remove(PROP_AUTO_COMMIT_ENABLE);
	}

	public String getAutoOffsetReset() {
		return map.get(PROP_AUTO_OFFSET_RESET);
	}
	public void setAutoOffsetReset(String autoOffsetReset) {
		map.put(PROP_AUTO_OFFSET_RESET,autoOffsetReset);
	}
	public void clearAutoOffsetReset() {
		map.remove(PROP_AUTO_OFFSET_RESET);
	}

	public String getAutoCommitIntervalMs() {
		return map.get(PROP_AUTO_COMMIT_INTERVAL_MS);
	}
	public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
		map.put(PROP_AUTO_COMMIT_INTERVAL_MS,autoCommitIntervalMs);
	}
	public void clearAutoCommitIntervalMs() {
		map.remove(PROP_AUTO_COMMIT_INTERVAL_MS);
	}

	public String getPartitionerClass() {
		return map.get(PROP_PARTITIONER_CLASS);
	}
	public void setPartitionerClass(String partitionClass) {
		map.put(PROP_PARTITIONER_CLASS, partitionClass);
	}
	public void clearPartitionerClass() {
		map.remove(PROP_PARTITIONER_CLASS);
	}

	public String getMessageSendMaxRetries() {
		return map.get(PROP_MESSAGE_SEND_MAX_RETRIES);
	}
	public void setMessageSendMaxRetries(String maxRetries) {
		map.put(PROP_MESSAGE_SEND_MAX_RETRIES, maxRetries);
	}
	public void clearMessageSendMaxRetries() {
		map.remove(PROP_MESSAGE_SEND_MAX_RETRIES);
	}

	public String getFetchMessageMaxBytes() {
		return map.get(PROP_FETCH_MESSAGE_MAX_BYTES);
	}
	public void setFetchMessageMaxBytes(String maxBytes) {
		map.put(PROP_FETCH_MESSAGE_MAX_BYTES, maxBytes);
	}
	public void clearFetchMessageMaxBytes() {
		map.remove(PROP_FETCH_MESSAGE_MAX_BYTES);
	}

	public String getRetryBackoffMilliseconds() {
		return map.get(PROP_RETRY_BACKOFF_MS);
	}
	public void setRetryBackoffMilliseconds(String numMilliseconds) {
		map.put(PROP_RETRY_BACKOFF_MS, numMilliseconds);
	}
	public void clearRetryBackoffMilliseconds() {
		map.remove(PROP_RETRY_BACKOFF_MS);
	}

	public String getBatchNumMessages() {
		return map.get(PROP_BATCH_NUM_MESSAGES);
	}
	public void setBatchNumMessages(String numMessages) {
		map.put(PROP_BATCH_NUM_MESSAGES, numMessages);
	}
	public void clearBatchNumMessages() {
		map.remove(PROP_BATCH_NUM_MESSAGES);
	}

	public String getQueueBufferingMaxMilliseonds() {
		return map.get(PROP_QUEUE_BUFFERING_MAX_MS);
	}
	public void setQueueBufferingMaxMilliseonds(String maxMilliseconds) {
		map.put(PROP_QUEUE_BUFFERING_MAX_MS, maxMilliseconds);
	}
	public void clearQueueBufferingMaxMilliseonds() {
		map.remove(PROP_QUEUE_BUFFERING_MAX_MS);
	}

	public String getQueueBufferingMaxMessages() {
		return map.get(PROP_QUEUE_BUFFERING_MAX_MESSAGES);
	}
	public void setQueueBufferingMaxMessages(String maxMessages) {
		map.put(PROP_QUEUE_BUFFERING_MAX_MESSAGES, maxMessages);
	}
	public void clearQueueBufferingMaxMessages() {
		map.remove(PROP_QUEUE_BUFFERING_MAX_MESSAGES);
	}

	public String getSendBufferBytes() {
		return map.get(PROP_SEND_BUFFER_BYTES);
	}
	public void setSendBufferBytes(String bytes) {
		map.put(PROP_SEND_BUFFER_BYTES, bytes);
	}
	public void clearSendBufferBytes() {
		map.remove(PROP_SEND_BUFFER_BYTES);
	}

	public String getCompressionCodec() {
		return map.get(PROP_COMPRESSION_CODEC);
	}
	public void setCompressionCodec(String codec) {
		map.put(PROP_COMPRESSION_CODEC,codec);
	}

	public ProducerConfig toProducerConfig() {
		return new ProducerConfig(toProperties());
	}

	public ConsumerConfig toConsumerConfig() {
		return new ConsumerConfig(toProperties());
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

	public Integer getSchemaVersion() {
		return this.schemaVersion;
	}
	public void setSchemaVersion(Integer schemaVersion) {
		this.schemaVersion = schemaVersion;
	}

	public String getSchemaRepoUrl() {
		return this.schemaRepoUrl;
	}
	public void setSchemaRepoUrl(String schemaRepoUrl) {
		this.schemaRepoUrl = schemaRepoUrl;
	}
	
}