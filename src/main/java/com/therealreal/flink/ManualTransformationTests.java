package com.therealreal.flink;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * This class reads data from kafka and transform date using a java function
 * @author gunjan.kaphle
 *
 */
public class ManualTransformationTests {
	
	private static String convertDate(String dateStr) throws ParseException {
		Date newDateObject = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(dateStr);
		
		DateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd");
		String output = outputFormatter.format(newDateObject);
		
		return output;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("testing");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test3");
		properties.setProperty("auto.offset.reset", "latest");      
		
		FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>("orders", 
				new JsonNodeDeserializationSchema(), properties);
		
		DataStream<ObjectNode> stream = env.addSource(kafkaSource);
		
		stream.map(new MapFunction<ObjectNode, ObjectNode>() {

			@Override
			public ObjectNode map(ObjectNode value) throws Exception {
				value.put("new_date", ManualTransformationTests.convertDate(value.get("timestamp").asText()));
				return value;
			}
			
		}).print();
		
		env.execute();
		
	}
	
}
