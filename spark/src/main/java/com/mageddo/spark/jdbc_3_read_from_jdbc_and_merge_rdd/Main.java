package com.mageddo.spark.jdbc_3_read_from_jdbc_and_merge_rdd;

import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

public class Main {

		public static void main(String[] args) {

			final Properties jdbcProps = new Properties();
			jdbcProps.put("user", "root");
			jdbcProps.put("password", "root");

			final JavaSparkContext sc = getContext();
			try(final SparkSession session = new SparkSession(sc.sc())){

				final Dataset<Row> dataSet = session.sqlContext()
					.read().jdbc("jdbc:mysql://mysql-server.dev:3306/TEMP", "USER", jdbcProps)
					.select("IDT_USER").where("NUM_AGE > 10");

				final JavaPairRDD<Integer, Row> missingUsers = dataSet.rdd().toJavaRDD()
					.keyBy(x -> x.getInt(0))
					.subtract(
							sc.parallelize(Arrays.asList((Row) new GenericRow(new Object[] { 1 }),
									new GenericRow(new Object[] { 2 })))
									.keyBy(x -> x.getInt(0))
					).sortByKey();

				missingUsers.foreachPartition(it -> {
					it.forEachRemaining(u -> System.out.println(u._1));
				});

			};
		}

		private static JavaSparkContext getContext() {

				final SparkConf sparkConf = new SparkConf()
				.setAppName("testWordCounter")
				.setMaster("local[2]")
				.set("spark.driver.allowMultipleContexts", "true")
				.set("spark.cassandra.connection.host", "cassandra.dev")
				.set("spark.cassandra.connection.port", "9042")
				.set("spark.cassandra.auth.username", "cassandra")
				.set("spark.cassandra.auth.password", "cassandra")
				.set("spark.cassandra.input.consistency.level", "ONE")
				.set("spark.cassandra.output.consistency.level", "ONE");
				return new JavaSparkContext(sparkConf);
		}
}
