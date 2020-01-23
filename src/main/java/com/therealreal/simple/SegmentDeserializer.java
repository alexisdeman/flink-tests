package com.therealreal.pubsub;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class SegmentDeserializer extends AbstractDeserializationSchema<PubSubEvent>{
	
	private static final long serialVersionUID = -5328874914204039056L;
	final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public PubSubEvent deserialize(byte[] message) throws IOException {
		System.out.println("Deserializing");
		JsonNode tree = objectMapper.readTree(new String(message));
		String event = tree.get("event").asText();
		String type = tree.get("type").asText();
		int userId = tree.get("userId").asInt();
		
		return new PubSubEvent(event, type, userId);
	}
	
	

}
