package com.mageddo.kafka.message.jmx;

import com.mageddo.kafka.message.RetryableKafkaListenerContainerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.kafka.config.AbstractKafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

@Component
@ManagedResource
public class TopicJMX {

	@Autowired
	private BeanFactory beanFactory;

	@Autowired
	private KafkaProperties kafkaProperties;

	@ManagedOperation
	public void processDLQ(String factoryName){
		final RetryableKafkaListenerContainerFactory factory = beanFactory.getBean(factoryName, RetryableKafkaListenerContainerFactory.class);
//		final MethodKafkaListenerEndpoint<Object, Object> endpoint = new MethodKafkaListenerEndpoint<>();
//		endpoint.setBean(factory.getBean());
//		factory.createListenerContainer(endpoint);
		final KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties()),
			new ContainerProperties("A"));

		container.is
	}
}
