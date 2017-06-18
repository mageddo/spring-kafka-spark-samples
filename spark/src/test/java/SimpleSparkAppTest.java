import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

//@RunWith()
public class SimpleSparkAppTest {


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

	private JavaSparkContext getContext() {

		final SparkConf sparkConf = new SparkConf()
			.setAppName("testWordCounter")
			.setMaster("local")
			.set("spark.driver.allowMultipleContexts", "true");
		return new JavaSparkContext(sparkConf);
	}

	private String cleanAndGetPath(String path) throws IOException {
		FileUtils.deleteDirectory(new File(path));
		return path;
	}


	@Test
	public void testAllWordCsounter() throws IOException {

		final JavaSparkContext sparkContext = getContext();
		final JavaRDD<String> wordFile = sparkContext.textFile("src/test/resources/testWordCounter.txt").cache();

		final JavaRDD<String> counts = wordFile
			.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		counts.saveAsTextFile(cleanAndGetPath("build/out_testAllWordsCounter"));

	}
}