package com.mageddo.jms.dao;

import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Created by elvis on 18/06/17.
 */

@Repository
public class YoutubeNotificationDAOH2 implements YoutubeNotificationDAO {

	@PersistenceContext
	private EntityManager entityManager;
}
