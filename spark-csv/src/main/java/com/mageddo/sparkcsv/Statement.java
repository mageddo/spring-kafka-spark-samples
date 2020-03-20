package com.mageddo.sparkcsv;

import java.math.BigDecimal;

public class Statement {

	private String description;
	private String customer;
	private BigDecimal amount;

	public String getDescription() {
		return description;
	}

	public Statement setDescription(String description) {
		this.description = description;
		return this;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public Statement setAmount(BigDecimal amount) {
		this.amount = amount;
		return this;
	}

	public String getCustomer() {
		return customer;
	}

	public Statement setCustomer(String customer) {
		this.customer = customer;
		return this;
	}
}
