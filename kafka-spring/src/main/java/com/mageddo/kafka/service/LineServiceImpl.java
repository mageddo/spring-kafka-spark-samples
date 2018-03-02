package com.mageddo.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Created by elvis on 18/06/17.
 */
@Service
@Transactional
public class LineServiceImpl implements LineService {

	@Autowired
	private KafkaTemplate kafkaTemplate;

	@Override
	@Transactional
	public void send(){
		kafkaTemplate.send("MyTopic", LocalDateTime.now().toString());
		System.out.println("sent");
	}

}
