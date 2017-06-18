package com.mageddo.jms.receiver;

import com.mageddo.jms.entity.YoutubeNotificationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class YoutubeNotificationReceiver {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private AtomicInteger subscriberdIds= new AtomicInteger(1);

	@Autowired
	private JmsTemplate jmsTemplate;

	public void doNotify() throws Exception {

	}

	public void consume(final YoutubeNotificationEntity notification) throws JMSException {

	}
}
