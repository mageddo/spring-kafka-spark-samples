package com.mageddo.spark.students.dao;

import com.mageddo.spark.students.vo.Student;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StudentDAO {

	private Connection connection;

	public StudentDAO(Connection connection) {
		this.connection = connection;
	}

	public List<Student> findAll() throws SQLException {

		try(PreparedStatement stm = connection.prepareStatement("SELECT ST.NAME, SC.NAME AS SCHOOL_NAME FROM STUDENT ST\n" +
			"\tINNER JOIN SCHOOL SC ON SC.ID = ST.SCHOOL_ID\n"+
			"\tORDER BY ST.NAME")){
			try(ResultSet rs = stm.executeQuery()){

				final List<Student> students = new ArrayList<>();
				while(rs.next()){
					students.add(new Student(rs.getString("NAME"), rs.getString("SCHOOL_NAME")));
				}
				return students;
			}
		}
	}

	public int countStudents() throws SQLException {

		try(PreparedStatement stm = connection.prepareStatement("SELECT COUNT(1) FROM STUDENT")){
			try(ResultSet rs = stm.executeQuery()){

				while(rs.next()){
					return rs.getInt(1);
				}
				return -1;
			}
		}
	}

	public int countSchools() throws SQLException {

		try(PreparedStatement stm = connection.prepareStatement("SELECT COUNT(1) FROM SCHOOL")){
			try(ResultSet rs = stm.executeQuery()){

				while(rs.next()){
					return rs.getInt(1);
				}
				return -1;
			}
		}
	}

	public void truncateDB() throws SQLException {
		try(PreparedStatement stm = this.connection.prepareStatement("TRUNCATE SCHEMA public AND COMMIT")){
			stm.executeUpdate();
		}
	}

}
