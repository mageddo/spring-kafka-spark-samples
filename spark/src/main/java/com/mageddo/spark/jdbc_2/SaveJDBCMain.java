package com.mageddo.spark.jdbc_2;


import com.mageddo.spark.jdbc_2.vo.Sale;
import com.mageddo.spark.jdbc_2.vo.SaleKey;
import com.mageddo.spark.jdbc_2.vo.SaleSummary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Random;

public class SaveJDBCMain {

	public static void main(String[] args) {

		final JavaSparkContext sparkContext = getContext();
		sparkContext.setLogLevel("ERROR");

		final JavaPairRDD<SaleKey, Iterable<SaleSummary>> salesSummary = sparkContext.parallelize(Arrays.asList(
			new Sale("West", "Apple", 2, 10),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15),
			new Sale("West", "Apple", 1, 15)
		))
			.map( v1 -> {
				return new SaleSummary(v1.store, v1.product, v1.amount, v1.units);
			})
			.keyBy( v1 -> {
				return new SaleKey(v1.product, v1.store);
			})
			.groupByKey(1); // <<< explicit partitions specify

		// the foreach will use the same partitions as groupBy
		salesSummary.foreachPartition((salesGroups -> {

			salesGroups.forEachRemaining(saleGroup -> {

				System.out.printf("key=%s%n", saleGroup._1); // save the key
				saleGroup._1.id = (long) new Random().nextInt(100_000);

				saleGroup._2.forEach(saleSummary -> {
					System.out.printf("\tid=%s, value=%s%n", saleGroup._1.id, saleSummary); // save the value
				});

			});
		}));


		sparkContext.stop();
	}

	private static JavaSparkContext getContext() {

		final SparkConf sparkConf = new SparkConf()
			.setAppName("testWordCounter")
			.setMaster("local[50]")
			.set("spark.driver.allowMultipleContexts", "true");
		return new JavaSparkContext(sparkConf);
	}
}
