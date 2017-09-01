package com.mageddo.spark.customio.vo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Movie implements Serializable, Writable {

	public String name;
	public Year year;

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

	@Override
	public void write(DataOutput out) throws IOException {
//		out.writeChars(this.name);
//		out.write(',');
		out.writeLong(Long.parseLong(this.name));
		out.writeInt(this.year.year);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = String.valueOf(in.readLong());
		this.year = new Year(in.readInt());
	}
}
