package com.therealreal.flink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


/**
 * Reads from Kafka topic using Simple String schema and prints the results
 * to stdout
 */
public class KafkaReads {

	public static void main(String[] args) throws Exception {
		System.out.println("testing");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test1");
		properties.setProperty("auto.offset.reset", "earliest");      
		
		FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("orders", 
				new SimpleStringSchema(), properties);
		
		DataStream<String> stream = env.addSource(kafkaSource);
		
		stream.print();
		
		env.execute();
		
	}
}
