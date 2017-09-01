package com.mageddo.spark.customio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mageddo.spark.customio.vo.Movie;
import com.mageddo.spark.customio.vo.Year;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CustomInput {

	public static void main(String[] args) {
		final JavaSparkContext sc = createContext();

//		final JavaPairRDD<LongWritable, Text> movies = sc.newAPIHadoopFile(
//			"/tmp/x2", MovieCustomTextInput.class, LongWritable.class, Text.class, sc.hadoopConfiguration()
//		);
		final JavaPairRDD<Year, Movie> movies = sc.newAPIHadoopRDD(
			sc.hadoopConfiguration(), MovieCustomInput.class, Year.class, Movie.class
		);

		movies.foreach(t -> {
			System.out.printf("year=%s, movie=%s%n", t._1, t._2);
		});

	}

	public static JavaSparkContext createContext(){
		final SparkConf sparkConf = new SparkConf();
		sparkConf
		.setAppName("WordCount")
		.setMaster("local[6]")
		;
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
		return sc;
	}

	public static class MovieCustomTextInput extends org.apache.hadoop.mapreduce.lib.input.TextInputFormat {

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {
//			file:/tmp/x2:0+4312
			return super.getSplits(job);
		}
	}

	public static class MovieCustomInput extends InputFormat<Year, Movie> implements Writable {

		@Override
		public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
			return Arrays.asList(new InputSplit() {
				@Override
				public long getLength() throws IOException, InterruptedException {
					return 1;
				}

				@Override
				public String[] getLocations() throws IOException, InterruptedException {
					return new String[]{"1"};
				}
			});
		}

		@Override
		public RecordReader<Year, Movie> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//			new LineRecordReader().
			return new MovieCustomReader();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			System.out.println("writing");
//			new ObjectMapper().writeValueAsBytes(this.)
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			System.out.println("reading");
		}
	}
}
