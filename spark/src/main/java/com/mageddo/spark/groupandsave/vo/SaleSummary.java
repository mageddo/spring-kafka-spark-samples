package com.mageddo.spark.groupandsave.vo;

import java.io.Serializable;

public class SaleSummary implements Serializable {

	public String store, product;
	public int amount, units;

	public SaleSummary(String store, String product, int amount, int units) {
		this.store = store;
		this.product = product;
		this.amount = amount;
		this.units = units;
	}

	@Override
	public String toString() {
		return "SaleSummary{" +
			"store='" + store + '\'' +
			", product='" + product + '\'' +
			'}';
	}
}