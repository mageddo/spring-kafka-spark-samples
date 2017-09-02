package com.mageddo.spark.students.vo;

import java.io.Serializable;

public class School implements Serializable {

	public int id;
	public String name;

	public School() {
	}

	public School(String name) {
		this.name = name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		School school = (School) o;

		return name.equals(school.name);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public String toString() {
		return "School{" +
			"id=" + id +
			", name='" + name + '\'' +
			'}';
	}
}
