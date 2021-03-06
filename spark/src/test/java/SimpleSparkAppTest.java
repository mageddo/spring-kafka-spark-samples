import com.mageddo.spark.jdbc_2.vo.Sale;
import com.mageddo.spark.jdbc_2.vo.SaleKey;
import com.mageddo.spark.jdbc_2.vo.SaleSummary;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

//@RunWith()
public class SimpleSparkAppTest {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void testWordOcurrenciesCounter() throws IOException {

		final JavaSparkContext sparkContext = getContext();
		final JavaRDD<String> wordFile = sparkContext.textFile("src/test/resources/testWordCounter.txt").cache();

		final JavaPairRDD<String, Integer> counts = wordFile
			.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			.mapToPair(word -> new Tuple2<>(word, 1))
			.reduceByKey((a, b) -> a + b);

		counts.saveAsTextFile(cleanAndGetPath("build/out_testWordCounter"));

	}

	@Test
	public void testAllWordsCounter() throws IOException {

		final JavaSparkContext sparkContext = getContext();
		final JavaRDD<String> wordFile = sparkContext.textFile("src/test/resources/testWordCounter.txt").cache();

		final JavaRDD<String> counts = wordFile
			.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		counts.saveAsTextFile(cleanAndGetPath("build/out_testAllWordsCounter"));

	}

	@Test
	public void testWordsAndNotify() throws IOException {

		final JavaSparkContext sparkContext = getContext();
//		final JavaRDD<String> wordFile = sparkContext.textFile("src/test/resources/testWordCounter.txt").cache();
		final JavaRDD<String> wordFile = sparkContext.textFile("/tmp/text").cache();

		final JavaPairRDD<String, Integer> counts = wordFile
			.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			.mapToPair(word -> new Tuple2<>(word, 1))
			.reduceByKey((a, b) -> a + b);

		counts.foreach(tuple -> {
			final String name = Thread.currentThread().getName();
			System.out.printf("tid=%-5s, key=%s, value=%s%n", name.substring(name.length() - 5, name.length()), tuple._1, tuple._2);
		});

	}
	@Test
	public void reduceGroupAndSaveToRelational() throws IOException {

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
		.map((Function<Sale, SaleSummary>) v1 -> {
			return new SaleSummary(v1.store, v1.product, v1.amount, v1.units);
		})
		.keyBy((Function<SaleSummary, SaleKey>) v1 -> {
			return new SaleKey(v1.product, v1.store);
		})
		.groupByKey(1); // <<< explicit partitions specify
//		.reduceByKey((Function2<SaleSummary, SaleSummary, SaleSummary>) (v1, v2) -> {
//			return new SaleSummary(v1.store, v1.product, v1.amount + v2.amount, v1.units + v2.units);
//		});

		// the foreach will use the same partitions as groupBy
		salesSummary.foreachPartition((VoidFunction<Iterator<Tuple2<SaleKey, Iterable<SaleSummary>>>>) salesGroups -> {

			salesGroups.forEachRemaining(saleGroup -> {

				System.out.printf("key=%s%n", saleGroup._1); // save the key
				saleGroup._1.id = (long) new Random().nextInt(100_000);

				saleGroup._2.forEach(saleSummary -> {
					System.out.printf("\tid=%s, value=%s%n", saleGroup._1.id, saleSummary); // save the value
				});

			});
		});

//		salesSummary.foreachPartition((VoidFunction<Iterator<Tuple2<SaleKey, SaleSummary>>>) salesByKey -> {
//			salesByKey.forEachRemaining(t -> {
//				// saving to the database
//				System.out.println(t._1 + ": " + t._2);
//			});
//		});

		sparkContext.stop();

	}



	private JavaSparkContext getContext() {

		final SparkConf sparkConf = new SparkConf()
			.setAppName("testWordCounter")
			.setMaster("local[50]")
			.set("spark.driver.allowMultipleContexts", "true");
		return new JavaSparkContext(sparkConf);
	}

	private String cleanAndGetPath(String path) throws IOException {
		FileUtils.deleteDirectory(new File(path));
		return path;
	}
}