package com.therealreal.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


/**
 * Reads from Kafka topic using Json schema and prints a field
 * from the json string to stdout
 */
public class FlinkFilters {

	public static void main(String[] args) throws Exception {
		System.out.println("testing");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test3");
		properties.setProperty("auto.offset.reset", "earliest");      
		
		FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>("orders", 
				new JsonNodeDeserializationSchema(), properties);
		
		DataStream<ObjectNode> stream = env.addSource(kafkaSource);
		
		stream.filter(new FilterFunction<ObjectNode>() {

			private static final long serialVersionUID = -4494456000370705069L;

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				if (value.get("price").asInt() > 1000) {
					return true;
				}
				return false;
			}
			
		}).print();
		
		env.execute();
		
	}
}
