package com.rakuten.dess.config;

public class TopicPartitionOffsetConfig {
	private String topic;
	private int partitionId;
	private long offset;

	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public int getPartitionId() {
		return partitionId;
	}
	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	
}
