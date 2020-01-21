package com.therealreal.tableapi;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * JOINS between two tables.
 * @author gunjan.kaphle
 *
 */
public class BasicJoins {
	
	private static Schema getOrderSchema() {
		return new Schema().field("order_id", "STRING")
			.field("user_id", "INT")
			.field("price", "DOUBLE")
			.field("collect_dt_tm", "TIMESTAMP")
			.field("rowtime", "TIMESTAMP")
	        .rowtime(new Rowtime()
    		 .timestampsFromSource()
	             .watermarksPeriodicBounded(60000)
	           );
	}
	
	private static Schema getUserSchema() {
		return new Schema().field("user_id", "INT")
			.field("name", "STRING")
	        .rowtime(new Rowtime());
	}

	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(10000L);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		getKafkaSource(tableEnv, "orders_join", getOrderSchema());
		getKafkaSource(tableEnv, "users_join", getUserSchema());
		
		// obtain query configuration from TableEnvironment
		TableConfig qConfig = tableEnv.getConfig();
		// set query parameters
		qConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));
		
		Table orders = tableEnv.scan("orders_join").select("user_id as order_user_id, order_id, price");
		Table users = tableEnv.scan("users_join").select("user_id as user_user_id, name");
		
		// 1. USING THE TABLE API
		Table result = orders.join(users).where("order_user_id = user_user_id").select(
				"user_user_id, name, order_id, price");
		
		// 2. USING THE QUERY API
		// Table result = joinTables(tableEnv);
		
		
		tableEnv.registerTable("full_orders", result);
		
		DataStream<Tuple2<Boolean, Row>> result2 = tableEnv.toRetractStream(result, Row.class);
		
		result2.print();
		
		env.execute();
	}
	
	
	private static Table joinTables(StreamTableEnvironment tableEnv) {
		return tableEnv.sqlQuery(
				"SELECT orders_join.user_id, orders_join.order_id, users_join.name, orders_join.price " +
				"FROM orders_join " +
				"JOIN users_join ON orders_join.user_id = users_join.user_id"
				);
	}

	private static void getKafkaSource(StreamTableEnvironment tableEnv, String topic, Schema givenSchema) {
		tableEnv.connect(
				new Kafka()
		        .version("universal")
		        .topic(topic)
		        .property("zookeeper.connect", "localhost:2181")
		        .property("bootstrap.servers", "localhost:9092")
		        .property("group.id", "flink")
		        .startFromEarliest()
		      )
   		  .withFormat(new Json().deriveSchema())
   		.withSchema(givenSchema)
	      .inAppendMode()
	      .registerTableSource(topic);
	}
}
