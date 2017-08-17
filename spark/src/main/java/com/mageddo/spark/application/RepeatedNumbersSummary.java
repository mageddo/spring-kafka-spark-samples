package com.mageddo.spark.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sum thre repeated numbers, you can use it as standalone spark or submit it to a cluster
 */
public class RepeatedNumbersSummary {

	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf();
		sparkConf
			.setAppName("WordCount")
			.set("spark.cores.max","1")
//			.setMaster("spark://localhost:3333");
//			.setMaster("spark://typer-pc:7077");
			.setMaster("local[*]")
			;

		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");

		final List<Integer> numbers = new ArrayList<>();
		for(int i=0; i < 100_000; i++){
			numbers.add(i);
		}

		sc.parallelize(numbers, 20)
		.map(n -> {
			return new Tuple2<>(n, 1);
		})
		.keyBy(n -> {
			return n._1;
		})
		.reduceByKey((Tuple2<Integer, Integer> n1, Tuple2<Integer, Integer> n2) -> {
			return new Tuple2<>(n1._1, n1._2 + n2._2);
		})
			.foreachPartition(it -> {

			AtomicInteger counter = new AtomicInteger(0);
			it.forEachRemaining(n -> {
//				System.out.printf("n=%d, count=%d", n._1, n._2._2);
				counter.incrementAndGet();
			});
			System.out.printf("counter=%d, thread=%s%n", counter.get(), Thread.currentThread().getId());
//				Thread.sleep(20000);
		})
		;

		sc.stop();

	}
}
