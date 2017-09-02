package com.mageddo.spark.students;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mageddo.spark.students.vo.School;
import com.mageddo.spark.students.vo.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Main {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static void main(String[] args) {
		final JavaSparkContext sc = createContext();

		final JavaPairRDD<School, Student> studentsPair =
//		sc.textFile("")
		sc.parallelize(Collections.nCopies(100, "{\"name\": \"A\", \"schoolName\": \"B\"}"))
		.mapToPair(line -> {
			final Student student = getMapper().readValue(line, Student.class);
			return new Tuple2<>(new School(student.schoolName), student);
		});

		final JavaPairRDD<School, Student> savedSchools = studentsPair
		.distinct()
		.mapPartitionsToPair(it -> {
			final Set<Tuple2<School, Student>> schools = new HashSet<>();
			it.forEachRemaining(tuple -> {

				School school = new School();
				school.id = new Random().nextInt(5000000);
				school.name = tuple._1.name;
				schools.add(new Tuple2<>(school, tuple._2));
			});
			return schools.iterator();
		});

		savedSchools.join(studentsPair)
		.foreachPartition(it -> {
			it.forEachRemaining(tuple -> {
				System.out.printf("school=%s, student=%s%n", tuple._1, tuple._2._2);
			});
		});

	}

	static JavaSparkContext createContext() {
		final SparkConf sparkConf = new SparkConf()
			.setAppName("WordCount")
			.setMaster("local");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
		return sc;
	}

	static ObjectMapper getMapper() {
		return MAPPER;
	}
}
