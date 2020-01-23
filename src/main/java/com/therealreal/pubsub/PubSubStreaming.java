package com.therealreal.pubsub;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;


public class PubSubStreaming {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000L);

		env.addSource(PubSubSource.newBuilder()
								  .withDeserializationSchema(new SimpleStringSchema())
								  .withProjectName("trr-cloudfunctions-test")
								  .withSubscriptionName("gunjan_flink_segment3")
								  .build())
        	.map(new MapFunction<String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String map(String value) throws Exception {
					System.out.println("Payload: " + value);
					return value;
				}
			});
		// can also use the map function this way through lambda function
		// .map(PubSubStreaming::printPayload)
		// .map(s -> PubSubStreaming.printPayload(s))

		env.execute("Flink Streaming PubSubReader");
	}
	
	private static String printPayload(String i) {
		System.out.println("Payload: " + i);
		return i;
	}

}