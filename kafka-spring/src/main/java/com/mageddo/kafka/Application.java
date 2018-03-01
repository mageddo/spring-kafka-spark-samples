package com.mageddo.kafka;

import com.mageddo.kafka.message.ConsumerDeclarer;
import com.mageddo.kafka.message.RetryableKafkaListenerContainerFactory;
import com.mageddo.kafka.message.TopicEnum;
import com.mageddo.kafka.service.LineService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;


@EnableKafka
@EnableScheduling
@EnableAspectJAutoProxy
@EnableAutoConfiguration

@SpringBootApplication
@Configuration
public class Application implements SchedulingConfigurer, InitializingBean {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private ConsumerDeclarer consumerDeclarer;

	public static void main(String[] args) {
		ConfigurableApplicationContext app = SpringApplication.run(Application.class);
//		LineService service = app.getBean(LineService.class);
//		service.send();
//		System.out.println("ok!");
	}

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.setScheduler(Executors.newScheduledThreadPool(5));
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		consumerDeclarer.declare(TopicEnum.values());
	}
}
