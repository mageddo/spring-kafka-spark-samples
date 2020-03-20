package com.mageddo.sparkcsv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Customer implements Serializable {

	private String name;
	private List<Statement> statements;

	public Customer(String name, List<Statement> statements) {
		this.name = name;
		this.statements = statements;
	}

	public String getName() {
		return name;
	}

	public Customer setName(String name) {
		this.name = name;
		return this;
	}

	public List<Statement> getStatements() {
		return statements;
	}

	public Customer setStatements(List<Statement> statements) {
		this.statements = statements;
		return this;
	}

	public static Customer of(String name, Iterable<Statement> statements) {
		final List<Statement> items = new ArrayList<>();
		statements.forEach(items::add);
		return new Customer(name, items);
	}
}
