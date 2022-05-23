package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import java.util.Arrays;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import twitter4j.FilterQuery;
import twitter4j.Logger;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class StreamRunnerImpl implements StreamRunner {
	
	private static final Logger LOG = Logger.getLogger(StreamRunnerImpl.class);
	
	TwitterKafkaStatusListener twitterKafkaStatusListener;
	TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	TwitterStream twitterStream;
	

	public StreamRunnerImpl(TwitterKafkaStatusListener twitterKafkaStatusListener,
			TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData) {
		this.twitterKafkaStatusListener = twitterKafkaStatusListener;
		this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
	}
	
	public void shutdown() {
		if(twitterStream !=null) {
			LOG.info("Twitter stream is going to close ....");
			twitterStream.shutdown();
		}
	}


	@Override
	public void start() throws TwitterException {
		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(twitterKafkaStatusListener);
		addFilter();
		
	}


	private void addFilter() {
		String filter_keys[] = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
		FilterQuery filterQuery = new FilterQuery(filter_keys);
		twitterStream.filter(filterQuery);
		LOG.info("Added Filter {} :",Arrays.toString(filter_keys));
	}

}
