package com.mageddo.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Created by elvis on 18/06/17.
 */
@Service
public class LineServiceImpl implements LineService {

	@Autowired
	private KafkaTemplate kafkaTemplate;

	@Override
	public void send(){
		kafkaTemplate.send("MyTopic", LocalDateTime.now().toString());
	}

}
