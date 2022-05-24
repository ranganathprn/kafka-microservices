package com.microservices.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "kafka-producer-confg")
@Data
public class KafkaProducerConfig {
	 private String  keySerializerClass;
	 private String valueSerializerClass;
	 private String compressionType;
	 private String acks;
	 private Integer batchSize;
	 private Integer batchSizeBoostFactor;
	 private Integer lingerMs;
	 private Integer requestTimeoutMs;
	 private Integer retryCount;
	

}
