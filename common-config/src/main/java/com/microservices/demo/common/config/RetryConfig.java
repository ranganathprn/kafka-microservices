package com.microservices.demo.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.microservices.demo.config.RetryConfigData;

@Configuration
public class RetryConfig {
	
	RetryConfigData retryConfigData;

	public RetryConfig(RetryConfigData retryConfigData) {
		this.retryConfigData = retryConfigData;
	}
	
	@Bean
	public RetryTemplate retryTemplate() {
		
		RetryTemplate retryTemplate = new RetryTemplate();
		
		ExponentialBackOffPolicy exponentitalBackOffPolicy = new ExponentialBackOffPolicy();
		
		exponentitalBackOffPolicy.setInitialInterval(retryConfigData.getInitialIntervalMs());
		exponentitalBackOffPolicy.setMaxInterval(retryConfigData.getMaxIntervalMs());
		exponentitalBackOffPolicy.setMultiplier(retryConfigData.getMultiplier());
		
		retryTemplate.setBackOffPolicy(exponentitalBackOffPolicy);
		
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
		simpleRetryPolicy.setMaxAttempts(retryConfigData.getMaxAttempts());
		retryTemplate.setRetryPolicy(simpleRetryPolicy);
		
		return retryTemplate;
		
		
	}
	

}
