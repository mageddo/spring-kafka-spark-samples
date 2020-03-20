package com.mageddo.sparkcsv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StatementsMain {
	public static void main(String[] args) throws JsonProcessingException {

		final List<Statement> statements = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			statements.add(
				new Statement()
					.setCustomer(String.format("user %d", new Random().nextInt(5)))
					.setDescription(String.format("selling %s", i))
					.setAmount(BigDecimal.valueOf(new Random().nextInt(50_0000)))
			);
		}

		final SparkConf sparkConf = new SparkConf()
			.setAppName("StatementsFileGenerator")
			.setMaster("local[5]");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");

		final List<Customer> customers = sc
			.parallelize(statements)
			.keyBy(Statement::getCustomer)
			.groupByKey()
			.map(it -> Customer.of(it._1, it._2))
			.collect();

		sc.stop();

		System.out.println(
			new ObjectMapper()
			.enable(SerializationFeature.INDENT_OUTPUT)
			.writeValueAsString(customers)
		);

	}
}
