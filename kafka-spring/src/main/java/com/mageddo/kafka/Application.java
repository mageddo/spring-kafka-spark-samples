package com.mageddo.kafka;

import com.mageddo.kafka.message.ConsumerDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@EnableKafka
@EnableScheduling
@EnableAspectJAutoProxy
@EnableAutoConfiguration

@SpringBootApplication
@EnableTransactionManagement
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
		consumerDeclarer.declare(Stream.of(TopicEnum.values()).map(i -> i.getTopic()).collect(Collectors.toList()));
	}

	@Bean
	public ConsumerDeclarer consumerDeclarer(ConfigurableBeanFactory beanFactory, KafkaProperties kafkaProperties,
																					 @Value("${spring.kafka.consumer.autostartup:true}") boolean autostartup){
		return new ConsumerDeclarer(beanFactory, kafkaProperties, autostartup);
	}

//
//	@Bean
//	public KafkaTransactionManager kafkaTransactionManager(ProducerFactory f) {
//		KafkaTransactionManager ktm = new KafkaTransactionManager(f);
//		ktm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
//		return ktm;
//	}
//
//	@Bean
//	@Primary
//	public DefaultKafkaProducerFactory x(KafkaProperties properties){
//		DefaultKafkaProducerFactory f = new DefaultKafkaProducerFactory<>(properties.buildProducerProperties());
//		f.setTransactionIdPrefix("myId");
//		return f;
//	}
//
//	// https://stackoverflow.com/questions/47354521/transaction-synchronization-in-spring-kafka
//	@Bean
//	@Primary
//	public JpaTransactionManager transactionManager(EntityManagerFactory em) {
//		return new JpaTransactionManager(em);
//	}
//
//	@Bean(name = "chainedTransactionManager")
//	public ChainedTransactionManager chainedTransactionManager(JpaTransactionManager jpaTransactionManager,
//																														 KafkaTransactionManager kafkaTransactionManager) {
//		return new ChainedTransactionManager(kafkaTransactionManager, jpaTransactionManager);
//	}
}
