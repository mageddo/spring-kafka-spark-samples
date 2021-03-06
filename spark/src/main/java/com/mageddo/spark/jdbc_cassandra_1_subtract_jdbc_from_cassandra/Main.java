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

			final JavaPairRDD<Integer, Row> cassandraRecords = CassandraJavaUtil.javaFunctions(sc)
				.cassandraTable("ps_accounting_adm", "user", new UserReaderFactory())
				.keyBy(r -> r.getInt(0));

				cassandraRecords.foreachPartition(it -> {
					it.forEachRemaining(u -> System.out.printf("id=%d,", u._2.getInt(0)));
				});

			final Dataset<Row> usersJdbc = session.sqlContext()
				.read().jdbc("jdbc:mysql://mysql-server.dev:3306/TEMP", "USER", jdbcProps)
				.select("IDT_USER").where("NUM_AGE > 10");
			final JavaPairRDD<Integer, Row> missingUsers = usersJdbc.rdd().toJavaRDD()
				.keyBy(x -> x.getInt(0))
				.subtract(cassandraRecords).sortByKey();

			missingUsers.foreachPartition(it -> {
				it.forEachRemaining(u -> System.out.printf("%d, ", u._1));
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

	static class UserReaderFactory implements RowReaderFactory<Row>, java.io.Serializable {

		@Override
		public RowReader<Row> rowReader(TableDef table, IndexedSeq<ColumnRef> selectedColumns) {
			return new RowReader<Row>() {

				@Override
				public Row read(com.datastax.driver.core.Row row, CassandraRowMetadata rowMetaData) {
					final GenericRow r = new GenericRow(new Object[]{Long.valueOf(row.getLong("idt_user")).intValue()});
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
	}
}
