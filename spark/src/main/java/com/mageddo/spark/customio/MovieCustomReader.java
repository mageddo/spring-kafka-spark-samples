package com.mageddo.spark.customio;

import com.mageddo.spark.customio.vo.Movie;
import com.mageddo.spark.customio.vo.Year;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Random;

public class MovieCustomReader extends RecordReader<Year, Movie> {

	private Year year;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		System.out.println("initialized");
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		year = new Year(2000 + new Random().nextInt(50));
		return new Random().nextInt() == 4;
	}

	@Override
	public Year getCurrentKey() throws IOException, InterruptedException {
		return year;
	}

	@Override
	public Movie getCurrentValue() throws IOException, InterruptedException {
		return new Movie(String.valueOf(System.currentTimeMillis()), year.year);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 1.0F;
	}

	@Override
	public void close() throws IOException {
		System.out.println("closed");
	}
}
