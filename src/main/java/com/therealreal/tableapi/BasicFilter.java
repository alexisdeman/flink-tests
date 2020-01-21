package com.therealreal.tableapi;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


public class BasicFilter {
	
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		getKafkaSource(tableEnv, "orders");
		
		Table results = tableEnv.sqlQuery("SELECT * FROM orders WHERE price > 5000");
		tableEnv.registerTable("orders2", results);
		
		
		//Table orders = tableEnv.scan("orders");
		
		DataStream<Row> result = tableEnv.toAppendStream(results, Row.class);
		result.print();
		
		env.execute();
	}
	
	
	private static void getKafkaSource(StreamTableEnvironment tableEnv, String topic) {
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
   		.withSchema(new Schema().field("order_id", "STRING")
   				.field("price", "DOUBLE"))
	      .inAppendMode()
	      .registerTableSource(topic);
	}
	
}
