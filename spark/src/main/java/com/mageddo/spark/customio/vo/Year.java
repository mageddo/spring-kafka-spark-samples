package com.mageddo.spark.customio.vo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

public class Year implements Serializable, Writable {

	public UUID id;
	public int year;

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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
	}
}
