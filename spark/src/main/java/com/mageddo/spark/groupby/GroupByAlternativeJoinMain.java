package com.mageddo.spark.groupby;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Sum the repeated numbers, you can use it as standalone spark or submit it to a cluster
 *
 * Save to jdbc database using foreach, use many partitions and the thread numbers same as connection pool size
 * If you put more threads than connections then will get "Timeout: Pool empty"
 *
 */
public class GroupByAlternativeJoinMain {

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
			numbers.add(new Movie(String.valueOf(i), new Random().nextInt(50_0000)));
		}

		final JavaPairRDD<Year, Movie> movies = sc.parallelize(numbers) // creating rdd from list
		// mapping the Years
		.mapToPair(m -> {
			return new Tuple2<>(m.year, m);
		});

		final JavaPairRDD<Year, Movie> yearsKeys = movies.distinct() // getting only the Years

		// saving the Years to database and mapping the saved id
		.mapPartitionsToPair(it -> {
			final List<Tuple2<Year, Movie>> years = new ArrayList<>();
			it.forEachRemaining(t -> {
				// simulating save
				t._1.id = UUID.randomUUID();
				years.add(t);
			});
			return years.iterator();
		});
//		System.out.printf("count=%d, years=%s%n", yearsKeys.count(), yearsKeys.collect());


		yearsKeys.join(movies) // join the movies with the saved Years
		.foreachPartition(it -> { // saving each movie correlating with the Year id
			it.forEachRemaining(t -> {
//				System.out.printf("years=%s, movie=%s%n", t._1, t._2._2);
			});
		});

		sc.stop();

	}

	static class Year implements Serializable {

		UUID id;
		int year;

		public Year(int year) {
			this.year = year;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Year year1 = (Year) o;

			return year == year1.year;
		}

		@Override
		public int hashCode() {
			return year;
		}

		@Override
		public String toString() {
			return "Year{" +
				"id=" + id +
				", year=" + year +
				'}';
		}
	}

	static public class Movie implements Serializable {

		String name;
		Year year;

		public Movie(String name, int year) {
			this.name = name;
			this.year = new Year(year);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Movie movie = (Movie) o;

			return year.equals(movie.year);
		}

		@Override
		public int hashCode() {
			return year.hashCode();
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
