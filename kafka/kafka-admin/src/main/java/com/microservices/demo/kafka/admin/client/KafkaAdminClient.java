package com.microservices.demo.kafka.admin.client;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;

@Component
public class KafkaAdminClient {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);
	private final RetryTemplate retryTemplate;
	private final KafkaConfigData kafkaConfigData;
	private final RetryConfigData retryConfigData;
	private final AdminClient adminClient;
	private final WebClient webClient;

	public KafkaAdminClient(RetryTemplate retryTemplate, KafkaConfigData kafkaConfigData,
			RetryConfigData retryConfigData, AdminClient adminClient, WebClient webClient) {
		this.retryTemplate = retryTemplate;
		this.kafkaConfigData = kafkaConfigData;
		this.retryConfigData = retryConfigData;
		this.adminClient = adminClient;
		this.webClient = webClient;
	}

	public void createTopics() {
		CreateTopicsResult createTopicsResult = null;
		try {
			createTopicsResult = retryTemplate.execute(this::doCreateTopics);
			LOG.info("Topics created Successfully!!!!");
		} catch (RuntimeException e) {
			LOG.error("Exception occured in creating the topics", e);
		}
	}

	private CreateTopicsResult doCreateTopics(RetryContext retrycontext1) {

		List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
		LOG.info("Creating {} topic(s), attempt {}", topicNames.size(), retrycontext1.getRetryCount());
		List<NewTopic> newTopics = topicNames.stream().map(topic -> new NewTopic(topic.trim(),
				kafkaConfigData.getNumOfPartitions(), kafkaConfigData.getReplicationFactor()))
				.collect(Collectors.toList());

		return adminClient.createTopics(newTopics);
	}

	private Collection<TopicListing> getTopics() {

		return retryTemplate.execute(this::getTopics);
	}

	private Collection<TopicListing> getTopics(RetryContext retrycontext1) {
		Collection<TopicListing> topicsListing = null;
		try {
			topicsListing = adminClient.listTopics().listings().get();
			if (topicsListing != null) {
				topicsListing.forEach(topic -> LOG.info("Topic name {}: ", topic.name()));
			}
		} catch (InterruptedException e) {
			LOG.error("InterruptedException occured while fetching topic names ", e);
		} catch (ExecutionException e) {
			LOG.error("ExecutionException occured while fetching topic names ", e);
		}

		return topicsListing;
	}

	public void checkTopicsCreated() {
		Collection<TopicListing> listTopics = getTopics();
		int retryCount = 1;
		Integer maxRetry = retryConfigData.getMaxAttempts();
		int multiplier = retryConfigData.getMultiplier().intValue();
		Long sleepTime = retryConfigData.getSleepTimeMs();

		for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
			while (!isTopicCreated(topic, listTopics)) {
				checkMaxRetry(retryCount++, maxRetry);
				sleep(sleepTime);
				sleepTime *= multiplier;
				listTopics = getTopics();

			}
		}

	}

	public void checkSchemaRegistry() {
		int retryCount = 1;
		Integer maxRetry = retryConfigData.getMaxAttempts();
		int multiplier = retryConfigData.getMultiplier().intValue();
		Long sleepTime = retryConfigData.getSleepTimeMs();
		while (!getSchemaRegistryStatus().is2xxSuccessful()) {
			checkMaxRetry(multiplier, retryCount);
			sleep(sleepTime);
			sleepTime *= multiplier;
		}
	}

	private HttpStatus getSchemaRegistryStatus() {
		try {
			return webClient.method(HttpMethod.GET).uri(kafkaConfigData.getSchemaRegistryUrl()).exchange()
					.map(response -> response.statusCode()).block();
		} catch (Exception e) {
			LOG.error("Exception occured during schema registry check", e);
			return HttpStatus.SERVICE_UNAVAILABLE;
		}
	}

	private void sleep(long sleepTimeMs) {
		try {
			Thread.sleep(sleepTimeMs);
		} catch (InterruptedException e) {
			LOG.error("Exception occured while waiting for creation of the new topics");
		}
	}

	private void checkMaxRetry(int currentRetry, int maxRetry) {
		if (currentRetry > maxRetry) {
			throw new RuntimeException("Reached max retry count in reading the topics, trouble shoot the issue..");
		}
	}

	private boolean isTopicCreated(String topicName, Collection<TopicListing> topicListing) {
		if (topicListing == null) {
			return false;
		}
		return topicListing.stream().anyMatch(topic -> topicName.equals(topic));
	}
}
