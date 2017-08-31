package com.mageddo.spark.groupby;

import com.mageddo.db.DBUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sum the repeated numbers, you can use it as standalone spark or submit it to a cluster
 *
 * Save to jdbc database using foreach, use many partitions and the thread numbers same as connection pool size
 * If you put more threads than connections then will get "Timeout: Pool empty"
 *
 */
public class GroupByAlternativeCartesianMain {

	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf();
		sparkConf
			.setAppName("WordCount")
			.setMaster("local[6]")
			;

		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");

		final List<Movie> numbers = new ArrayList<>();
		for(int i=0; i < 1_000_000; i++){
			numbers.add(new Movie(String.valueOf(i), new Random().nextInt(50_000)));
		}

		final JavaRDD<Movie> numbersRdd = sc.parallelize(numbers);

		final JavaRDD<Integer> keys = numbersRdd.map(m -> {
			return m.year;
		}).distinct();

//		System.out.printf("keys=%s%n", keys.collect());

		numbersRdd.cartesian(keys).foreach(t -> {
			if(t._2.equals(t._1.year)){
				System.out.printf("m=%s%n", t._1);
			}
		});

		sc.stop();

	}

	static public class Movie implements Serializable {

		String name;
		int year;

		public Movie(String name, int year) {
			this.name = name;
			this.year = year;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Movie movie = (Movie) o;

			return year == movie.year;
		}

		@Override
		public int hashCode() {
			return year;
		}

		@Override
		public String toString() {
			return "Movie{" +
				"name='" + name + '\'' +
				", year=" + year +
				'}';
		}
	}

}
