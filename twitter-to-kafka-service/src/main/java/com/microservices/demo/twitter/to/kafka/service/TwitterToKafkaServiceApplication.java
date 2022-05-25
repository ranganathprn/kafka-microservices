package com.microservices.demo.twitter.to.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.init.StreamInitializer;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
	
	TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	
	public static final Logger LOG= LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
	private final StreamRunner streamRunner;
	private final StreamInitializer streamInitializer;
	
	public TwitterToKafkaServiceApplication(StreamInitializer streamInitializer, StreamRunner streamRunner) {
		this.streamInitializer = streamInitializer;
		this.streamRunner = streamRunner;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		LOG.info("Application starts....{}", twitterToKafkaServiceConfigData.getWelcomeMessage());
		streamInitializer.init();
		streamRunner.start();
		
	}

}
