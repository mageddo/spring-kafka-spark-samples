package com.mageddo.jms.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Created by elvis on 15/06/17.
 */
@Entity
@Table(name = "YOUTUBE_NOTIFICATION")
public class YoutubeNotificationEntity {

	@Column(name = "IDT_YOUTUBE_NOTIFICATION")
	@Id
	private Integer id;

	@Column(name = "IDT_USER")
	private Integer to;

	public YoutubeNotificationEntity() {
	}

	public YoutubeNotificationEntity(int to) {
		this.to = to;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getTo() {
		return to;
	}

	public void setTo(Integer to) {
		this.to = to;
	}
}
