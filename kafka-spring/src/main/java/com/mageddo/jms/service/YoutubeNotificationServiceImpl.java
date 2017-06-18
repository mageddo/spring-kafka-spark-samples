package com.mageddo.jms.service;

import com.mageddo.jms.dao.YoutubeNotificationDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by elvis on 18/06/17.
 */
@Service
public class YoutubeNotificationServiceImpl implements YoutubeNotificationService {

	@Autowired
	private YoutubeNotificationDAO youtubeNotificationDAO;

}
