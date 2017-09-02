package com.mageddo.spark.students.vo;

import java.io.Serializable;

public class Student implements Serializable {

	public int id;
	public String name;
	public String schoolName;

	public Student() {
	}

	public Student(String name, String schoolName) {
		this.name = name;
		this.schoolName = schoolName;
	}


	@Override
	public String toString() {
		return "Student{" +
			"id=" + id +
			", name='" + name + '\'' +
			", schoolName='" + schoolName + '\'' +
			'}';
	}
}
