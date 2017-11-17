package com.mageddo.spark.sparkstream_1;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.time.Duration;

import static org.apache.spark.streaming.Duration.apply;


public class Main {

	/**
	 * Just drop text files at the specified folder and see it being loaded
	 */
	public static void main(String[] args) throws InterruptedException {

		final JavaStreamingContext sc = getContext(Duration.ofSeconds(2));
		sc.textFileStream("/tmp/")
		.map(x -> x)
		.foreachRDD(rdd -> {
			rdd.foreachPartition(it -> it.forEachRemaining(System.out::println));
		});
		sc.start();
		sc.awaitTermination();
	}

	private static JavaStreamingContext getContext(Duration duration) {
		final SparkConf conf = new SparkConf()
			.setAppName("SparkFileReader")
			.setMaster("local[10]");
		final JavaStreamingContext ssc = new JavaStreamingContext(conf, apply(duration.toMillis()));
		ssc.sparkContext().setLogLevel("ERROR");
		return ssc;
	}
}
