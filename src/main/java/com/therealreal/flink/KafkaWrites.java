package com.therealreal.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

/**
 * Reads from kafka, filters data out and writes back to kafka
 * @author gunjan.kaphle
 *
 */
public class KafkaWrites {
	
	public static void main(String[] args) throws Exception {
		System.out.println("testing");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test3");
		properties.setProperty("auto.offset.reset", "earliest");      
		
		FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>("orders", 
				new JsonNodeDeserializationSchema(), properties);
		
		DataStream<ObjectNode> stream = env.addSource(kafkaSource);
		
		// TODO: look into the deprecation of the producer and find alternatives
		FlinkKafkaProducer<ObjectNode> producer = new FlinkKafkaProducer<>("filtered_orders", 
				new SerializationSchema() {

					@Override
					public byte[] serialize(Object element) {
						return element.toString().getBytes();
					}
				
				}, properties);
		
		stream.filter(new FilterFunction<ObjectNode>() {


			@Override
			public boolean filter(ObjectNode value) throws Exception {
				if (value.get("price").asInt() > 1000) {
					return true;
				}
				return false;
			}
			
		}).addSink(producer);
		
		env.execute();
	}
	
}
