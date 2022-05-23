package com.microservices.demo.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {

	public List<String> twitterKeywords;
	public String  welcomeMessage;
	Integer mockMinTweetLength;
	Integer mockMaxTweetLength;
	Long mockSleepMs;
	Boolean enableMockTweets;
	
	
}
