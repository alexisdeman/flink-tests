package com.therealreal.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;

public class KafkaWritesJson {

	
	public static void main(String[] args) throws Exception {
		System.out.println("testing");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test5");
		properties.setProperty("auto.offset.reset", "earliest");      
		
		
		//FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>("orders", 
		//		new JsonNodeDeserializationSchema(), properties);
		
		FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<ObjectNode>("orders", 
				new JSONKeyValueDeserializationSchema(false), properties);
		
		
		DataStream<ObjectNode> stream = env.addSource(kafkaSource);
		
		
		stream.rebalance().map(new MapFunction<ObjectNode, String>() {

			private static final long serialVersionUID = 8792632097172585870L;

			@Override
			public String map(ObjectNode node) throws Exception {
				return "Got message from kafka: " + node.get("key").get("index").asInt();
			}
			
		}).print();
		
		env.execute();
	}
	
}
