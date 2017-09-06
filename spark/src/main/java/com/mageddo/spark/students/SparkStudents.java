package com.mageddo.spark.students;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mageddo.spark.students.vo.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static com.mageddo.spark.students.DBUtils.getConnection;

public class SparkStudents {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public void groupAndTransport(String jsonPath) throws Exception {

		final JavaSparkContext sc = createContext();
		final JavaPairRDD<String, Student> studentsPair = sc.textFile(jsonPath)
		.mapToPair(line -> {
			final Student student = getMapper().readValue(line, Student.class);
			return new Tuple2<>(student.name, student);
		})
		.reduceByKey((s1, s2) -> s1) // removing duplicated students
		.mapToPair(tuple -> new Tuple2<>(tuple._2.schoolName, tuple._2))
		;

		final JavaPairRDD<String, Student> savedSchools = studentsPair

		// Remove all duplicated schools
		.reduceByKey((s1, s2) -> s1)

		// Saving each school and mapping it id back
		.mapPartitionsToPair(it -> {

			final Set<Tuple2<String, Student>> schools = new HashSet<>();
			try(Connection conn = getConnection()){

				it.forEachRemaining(tuple -> {

					try(PreparedStatement stm = conn.prepareStatement("INSERT INTO SCHOOL (NAME) VALUES (?)", Statement.RETURN_GENERATED_KEYS)){

						stm.setString(1, tuple._2.schoolName);
						stm.executeUpdate();

						try(ResultSet rs = stm.getGeneratedKeys()){
							if(rs.next()){
								tuple._2.schoolId = rs.getInt(1);
							}else {
								throw new IllegalStateException();
							}
						}
						schools.add(new Tuple2<>(tuple._1, tuple._2));

					}catch (Exception e){
						throw new RuntimeException(e);
					}
				});
				conn.commit();
			}
			return schools.iterator();
		});

		savedSchools.join(studentsPair)
		.foreachPartition(it -> {
			try(Connection conn = getConnection()){

				it.forEachRemaining(tuple -> {

					try(PreparedStatement stm = conn.prepareStatement("INSERT INTO STUDENT (SCHOOL_ID, NAME) VALUES (?, ?)")){

						stm.setInt(1, tuple._2._1.schoolId);
						stm.setString(2, tuple._2._2.name);
						stm.executeUpdate();

					}catch (Exception e){
						throw new RuntimeException(e);
					}

				});
				conn.commit();
			}

		});

		sc.close();

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
