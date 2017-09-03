package com.mageddo.spark.students.vo;

import java.io.Serializable;

public class Student implements Serializable {

	public int id;
	public String name;

	public int schoolId;
	public String schoolName;

	public Student() {
	}

	public Student(String name, String schoolName) {
		this.name = name;
		this.schoolName = schoolName;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Student student = (Student) o;

		return schoolName.equals(student.schoolName);
	}

	@Override
	public int hashCode() {
		return schoolName.hashCode();
	}

	@Override
	public String toString() {
		return "Student{" +
			"id=" + id +
			", name='" + name + '\'' +
			", schoolId=" + schoolId +
			", schoolName='" + schoolName + '\'' +
			'}';
	}
}
