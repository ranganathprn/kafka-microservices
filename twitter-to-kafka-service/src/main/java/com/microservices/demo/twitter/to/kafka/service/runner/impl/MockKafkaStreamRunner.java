package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue="true")
public class MockKafkaStreamRunner implements StreamRunner {

	private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	private final TwitterKafkaStatusListener twitterKafkaStatusListener;

	public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
			TwitterKafkaStatusListener twitterKafkaStatusListener) {
		this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
		this.twitterKafkaStatusListener = twitterKafkaStatusListener;
	}

	private static final String[] WORDS = new String[] { "Lorem", "ipsum", "dolor", "sit", "amet", "consectetuer",
			"adipiscing", "elit", "Maecenas", "porttitor", "congue", "massa", "Fusce", "posuere", "magna", "sed",
			"pulvinar", "ultricies", "purus", "lectus", "malesuada", "libero" };
	private static final Random RANDOM = new Random();

	private static final String tweetAsRawJson = "{" + "\"created_at\":\"{0}\"," + "\"id\":\"{1}\","
			+ "\"text\":\"{2}\"," + "\"user\":{\"id\":\"{3}\"}" + "}";
	private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

	@Override
	public void start() throws TwitterException {
		final String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
		Integer mockMaxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
		Integer mockMinTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
		Long mockSleepMs = twitterToKafkaServiceConfigData.getMockSleepMs();
		LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords)); 
		simulateTwitterStream(keywords, mockMaxTweetLength, mockMinTweetLength, mockSleepMs);

	}

	private void simulateTwitterStream(String[] keywords, int mockMaxTweetLength, int mockMinTweetLength,
			long mockSleepMs) {
		Executors.newSingleThreadExecutor().submit(() -> {
			while (true) {
				String formattedTextAsJson = getFormattedTweet(keywords, mockMinTweetLength, mockMaxTweetLength,
						mockSleepMs);
				LOG.info("Formatted JSON Object: {}", formattedTextAsJson);
				try {
					Status status = TwitterObjectFactory.createStatus(formattedTextAsJson);
					twitterKafkaStatusListener.onStatus(status);
					sleep(mockSleepMs);
				} catch (TwitterException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

	}

	private void sleep(long mockSleepMs) {

		try {
			Thread.sleep(mockSleepMs);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	private String getFormattedTweet(String[] keywords, int mockMinTweetLength, int mockMaxTweetLength,
			long sleepTime) {
		String[] params = new String[] {
				ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
				getRandomTweetContent(keywords, mockMinTweetLength, mockMaxTweetLength),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)) };
		LOG.info("Params for the mock tweet response {}", Arrays.toString(params));	
		return formatTweetAsJsonWithParams(params);
	}

	private String formatTweetAsJsonWithParams(String[] params) {

		String tweet = tweetAsRawJson;
		for (int i = 0; i < params.length; i++) {
			tweet = tweet.replace("{" + i + "}", params[i]);
		}
		LOG.info("After filling the place holders in the formatted template : {}",tweet);
		return tweet;
	}

	private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
		StringBuilder tweet = new StringBuilder();
		int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + maxTweetLength;
		LOG.info("Max length after manipulation {} : ", String.valueOf(tweetLength));
		return constructRandomTweet(keywords, tweet, tweetLength);
	}

	private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {

		for (int i = 0; i < tweetLength; i++) {
			tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
			if (i == tweetLength / 2) {
				tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
			}
		}
		return tweet.toString();

	}

}
