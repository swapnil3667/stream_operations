package com.rakuten.dess.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import scala.collection.JavaConversions;

public class Config implements Serializable {
	private static final long serialVersionUID = 1L;

	private List<TopicPartitionOffsetConfig> startOffsets;
	private Boolean useFirstOffsets = false;


	private String sparkAppName;

	private String kinesisStreamName;

	private String kinesisDynamoDbName;

	private String kinesisEndpointUrl;


	private String sparkMaster;
	private Map<String,String> sparkConfig;

	private Long sparkBatchIntervalSeconds;

	private String checkPointDir;

	private String awsAccessKey;
	private String awsSecretKey;

	private Long checkPointIntervalSeconds;

	//private KinesisConsumerConfig kafkaConsumerConfig;
	private KafkaProducerConfig kafkaProducerConfig;

	private String shutdownFile = null;

	public Map<String, String> getSparkConfig() {
		return sparkConfig;
	}

	public String getKinesisEndpointUrl() {
		return kinesisEndpointUrl;
	}

	public void setKinesisEndpointUrl(String kinesisEndpointUrl) {
		this.kinesisEndpointUrl = kinesisEndpointUrl;
	}
	public String getKinesisStreamName() {
		return kinesisStreamName;
	}

	public void setKinesisStreamName(String kinesisStreamName) {
		this.kinesisStreamName = kinesisStreamName;
	}

	public String getKinesisDynamoDbName() {
		return kinesisDynamoDbName;
	}

	public void setKinesisDynamoDbName(String kinesisDynamoDbName) {
		this.kinesisDynamoDbName = kinesisDynamoDbName;
	}

	public void setSparkConfig(Map<String, String> sparkConfig) {
		this.sparkConfig = sparkConfig;
	}

	public String getSparkMaster() {
		return sparkMaster;
	}
	public void setSparkAppName(String sparkAppName) {
		this.sparkAppName = sparkAppName;
	}

	public String getSparkAppName() {
		return sparkAppName;
	}
	public void setSparkMaster(String sparkMaster) {
		this.sparkMaster = sparkMaster;
	}

	public Long getSparkBatchIntervalSeconds() {
		return sparkBatchIntervalSeconds;
	}
	public void setSparkBatchIntervalSeconds(Long sparkBatchIntervalSeconds) {
		this.sparkBatchIntervalSeconds = sparkBatchIntervalSeconds;
	}

	public String getCheckPointDir() {
		return checkPointDir;
	}

	public void setCheckPointDir(String checkPointDir) {
		this.checkPointDir = checkPointDir;
	}

	public Long getCheckPointIntervalSeconds() {
		return checkPointIntervalSeconds;
	}
	public void setCheckPointIntervalSeconds(Long checkPointIntervalSeconds) {
		this.checkPointIntervalSeconds = checkPointIntervalSeconds;
	}

	public KafkaProducerConfig getKafkaProducerConfig() {
		return kafkaProducerConfig;
	}
	public void setKafkaProducerConfig(KafkaProducerConfig kafkaConfig) {
		this.kafkaProducerConfig = kafkaConfig;
	}

	public List<TopicPartitionOffsetConfig> getStartOffsets() {
		return startOffsets;
	}
	public void setStartOffsets(List<TopicPartitionOffsetConfig> startOffsets) {
		this.startOffsets = startOffsets;
	}

	public Boolean getUseFirstOffsets() {
		return useFirstOffsets;
	}
	public void setUseFirstOffsets(Boolean useFirstOffsets) {
		this.useFirstOffsets = useFirstOffsets;
	}

	public String getShutdownFile() {
		return shutdownFile;
	}
	public void setShutdownFile(String shutdownFile) {
		this.shutdownFile = shutdownFile;
	}

	public String getAwsAccessKey() {
		return awsAccessKey;
	}

	public void setAwsAccessKey(String awsAccessKey) {
		this.awsAccessKey = awsAccessKey;
	}

	public String getAwsSecretKey() {
		return awsSecretKey;
	}

	public void setAwsSecretKey(String awsSecretKey) {
		this.awsSecretKey = awsSecretKey;
	}


	public static <T extends Config> T load(File config, Class<T> configClazz) throws FileNotFoundException {
		Yaml yaml = new Yaml(new Constructor(configClazz));
		return configClazz.cast(yaml.load(new FileInputStream(config)));
	}

	public static Config load(String configFile) throws FileNotFoundException {
		return load(new File(configFile),Config.class);
	}

	public static <T extends Config> T load(String configFile, Class<T> configClazz) throws FileNotFoundException {
		return load(new File(configFile),configClazz);
	}

	public static SparkConf sparkConfig(final Config config) {
		SparkConf sparkConf = new SparkConf().setAppName(config.getSparkAppName());

		if (config.getSparkConfig() != null) {
			sparkConf = sparkConf.setAll(JavaConversions.mapAsScalaMap(config.getSparkConfig()));
		}

		if (config.getSparkMaster() != null) {
			sparkConf = sparkConf.setMaster(config.getSparkMaster());
		}

		return sparkConf;
	}
} 
