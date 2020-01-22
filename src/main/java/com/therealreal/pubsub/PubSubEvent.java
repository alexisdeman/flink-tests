package com.therealreal.pubsub;
import org.apache.flink.api.java.functions.KeySelector;

public class PubSubEvent {
	public String event;
	public String type;
	public int userId;
	
	public PubSubEvent(String event,String type, int userId) {
		this.event = event;
		this.type = type;
		this.userId = userId;
	}
	
	public String toString() {
		return "Event: " + this.event + " Type: " + this.type + " User: " + this.userId;
	}
	
}
