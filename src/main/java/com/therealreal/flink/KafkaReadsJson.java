package com.therealreal.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;


/**
 * Reads from Kafka topic using Json deserialization schema and prints a field
 * from the json string to stdout
 */
public class KafkaReadsJson {
	

	public static void main(String[] args) throws Exception {
		System.out.println("testing");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test3");
		properties.setProperty("auto.offset.reset", "earliest");      
		
		// TODO Look why JSONKeyValueDeserializationSchema is not working
		FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>("orders", 
				new JsonNodeDeserializationSchema(), properties);
		
		DataStream<ObjectNode> stream = env.addSource(kafkaSource);
		
		stream.map(new MapFunction<ObjectNode, String>() {

			@Override
			public String map(ObjectNode value) throws Exception {
				return "Got message from kafka: " + value.get("price");
			}
			
		}).print();
		
		env.execute();
		
	}
}
