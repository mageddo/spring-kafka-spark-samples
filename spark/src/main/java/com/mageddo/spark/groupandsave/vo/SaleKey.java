package com.mageddo.spark.groupandsave.vo;

import java.io.Serializable;

public class SaleKey implements Serializable {

	public Long id;
	public String store, product;

	public SaleKey(String store, String product) {
		this.store = store;
		this.product = product;
	}

	@Override
	public boolean equals(Object obj) {
		return this.hashCode() == obj.hashCode();
	}

	@Override
	public int hashCode() {
		int result = store.hashCode();
		result = 31 * result + product.hashCode();
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder()//
			.append("SaleKey [")//
			.append("id=")//
			.append(id)//
			.append(",store=\"")//
			.append(store).append("\"")//
			.append(",product=\"")//
			.append(product).append("\"")//
			.append("]");
		return builder.toString();
	}
}