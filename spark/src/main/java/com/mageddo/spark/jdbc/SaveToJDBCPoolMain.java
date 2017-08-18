package com.mageddo.spark.jdbc;

import com.mageddo.db.DBUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sum the repeated numbers, you can use it as standalone spark or submit it to a cluster
 *
 * Save to jdbc database using foreach, use many partitions and the thread numbers same as connection pool size
 * If you put more threads than connections then will get "Timeout: Pool empty"
 *
 */
public class SaveToJDBCPoolMain {

	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf();
		sparkConf
			.setAppName("WordCount")
//			.set("spark.cores.max","3")
//			.setMaster("spark://localhost:3333");
//			.setMaster("spark://typer-pc:7077");
			.setMaster("local[2]")
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
		.repartition(50)
		.foreachPartition(it -> {
			System.out.printf("partition=%s\n", Thread.currentThread().getName());
			try(final Connection conn = DBUtils.getConnection()){
				final AtomicInteger counter = new AtomicInteger(0);
				it.forEachRemaining(n -> {
						counter.incrementAndGet();
				});
				// the thread number may change (can have more different numbers than thread concurrent size) it's normal
				// anyway the parallel thread numbers will respect the master configuration []
				System.out.printf("\tcounter=%d, thread=%s%n", counter.get(), Thread.currentThread().getId());
			}

		})
		;

		sc.stop();

	}
}
