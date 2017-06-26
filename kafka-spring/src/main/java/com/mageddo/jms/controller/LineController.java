package com.mageddo.jms.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by elvis on 18/06/17.
 */

@Controller
public class LineController {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@RequestMapping(value = "/notify", method = RequestMethod.GET)
	public @ResponseBody void notification(String line){
		logger.info("line={}", line);
	}
}
