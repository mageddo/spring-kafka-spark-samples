package com.mageddo.spark.group_and_save_jdbc.vo;

import java.io.Serializable;

public class Sale implements Serializable {

	public String store, product;
	public int amount, units;

	public Sale(String store, String product, int amount, int units) {
		this.store = store;
		this.product = product;
		this.amount = amount;
		this.units = units;
	}
}