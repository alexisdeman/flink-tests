package com.therealreal.tableapi;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


public class BasicGrouping {
	
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(10000L);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		getKafkaSource(tableEnv, "orders.v4");
		
		Table results = tableEnv.sqlQuery("SELECT user_id, sum(price) AS total_price, "
				+ "TUMBLE_START(rowtime, INTERVAL '1' MINUTE) as wStart "
				+ "FROM orders_v2 "
				+ "GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), user_id");
		tableEnv.registerTable("orders5", results);
		
		//Table orders = tableEnv.scan("orders");
		
		//GroupWindowedTable windowedTable = results.window(Tumble.over("10.minutes").on("rowtime").as("rowtimeWindow"));
		
		// TODO: look into why  toUpdateStream is not working
		DataStream<Tuple2<Boolean, Row>> result2 = tableEnv.toRetractStream(results, Row.class);
		
		//DataStream<Row> result = tableEnv.toAppendStream(results, Row.class);
		result2.print();
		
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
   				.field("user_id", "INT")
   				.field("price", "DOUBLE")
   				.field("collect_dt_tm", "TIMESTAMP")
   				.field("rowtime", "TIMESTAMP")
   		        .rowtime(new Rowtime()
	        		 .timestampsFromSource()
   		             .watermarksPeriodicBounded(60000)
   		           ))
	      .inAppendMode()
	      .registerTableSource("orders_v2");
	}
	
}
