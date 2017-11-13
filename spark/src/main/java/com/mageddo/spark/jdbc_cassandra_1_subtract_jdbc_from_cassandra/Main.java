package com.mageddo.spark.jdbc_cassandra_1_subtract_jdbc_from_cassandra;

import java.util.Properties;

import com.datastax.spark.connector.CassandraRowMetadata;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import scala.Option;
import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class Main {

	public static void main(String[] args) {

		final Properties jdbcProps = new Properties();
		jdbcProps.put("user", "root");
		jdbcProps.put("password", "root");

		final JavaSparkContext sc = getContext();
		try(final SparkSession session = new SparkSession(sc.sc())){

			final Dataset<Row> dataSet = session.sqlContext()
				.read().jdbc("jdbc:mysql://mysql-server.dev:3306/TEMP", "USER", jdbcProps)
				.select("IDT_USER").where("NUM_AGE > 10");

			final RowReaderFactory<Row> readerFactory = new RowReaderFactory<Row>() {
				@Override
				public RowReader<Row> rowReader(TableDef table, IndexedSeq<ColumnRef> selectedColumns) {
					return new RowReader<Row>() {

						@Override
						public Row read(com.datastax.driver.core.Row row, CassandraRowMetadata rowMetaData) {
							final GenericRow r = new GenericRow(row.getInt("idt_user"));
							return r;
						}

						@Override
						public Option<Seq<ColumnRef>> neededColumns() {
							return Option.empty();
						}
					};
				}

				@Override
				public Class<Row> targetClass() {
					return Row.class;
				}
			};
			
			final JavaPairRDD<Integer, Row> cassandraRecords = CassandraJavaUtil.javaFunctions(sc)
				.cassandraTable("ps_accounting_adm", "user", readerFactory)
				.keyBy(r -> r.getInt(0));
//				.cassandraTable("ps_accounting_adm", "user", CassandraJavaUtil.mapRowTo(User.class))
//				.select(
//					column("idt_user").as("id")
//				);

//				.foreachPartition(it -> {
//					it.forEachRemaining(u -> System.out.println("id" + u.getInt(0)));
//				});

			final JavaPairRDD<Integer, Row> missingUsers = dataSet.rdd().toJavaRDD()
				.keyBy(x -> x.getInt(0))
				.subtract(cassandraRecords).sortByKey();

			missingUsers.foreachPartition(it -> {
				it.forEachRemaining(u -> System.out.println(u._1));
			});

		};
	}

	private static JavaSparkContext getContext() {

		final SparkConf sparkConf = new SparkConf()
			.setAppName("testWordCounter")
			.setMaster("local[2]")
			.set("spark.driver.allowMultipleContexts", "true")
			.set("spark.cassandra.connection.host", "cassandra.dev")
			.set("spark.cassandra.connection.port", "9042")
			.set("spark.cassandra.auth.username", "cassandra")
			.set("spark.cassandra.auth.password", "cassandra")
			.set("spark.cassandra.input.consistency.level", "ONE")
			.set("spark.cassandra.output.consistency.level", "ONE");
		return new JavaSparkContext(sparkConf);
	}
}
