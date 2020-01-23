package com.therealreal.pubsub;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.util.Collector;

public class PubSubStreaming2 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.enableCheckpointing(4000);
		String subscriptionName = "projects//trr-cloudfunctions-test//subscriptions//gunjan_flink_segment";
		
		SegmentDeserializer deserializer = new SegmentDeserializer();
		Map<String, String> envVars = System.getenv();
		String applicationFile = envVars.get("GOOGLE_APPLICATION_CREDENTIALS");
		String pubsub_subs = envVars.get("PUBSUB_SUBSCRIPTION");
		System.out.println(applicationFile);
		System.out.println(pubsub_subs);
		String project = "trr-cloudfunctions-test";
		String subscription = "gunjan_flink_segment2";
		
		SourceFunction<PubSubEvent> pubsubSource = PubSubSource.newBuilder()
				.withDeserializationSchema(deserializer)
				.withProjectName(project)
				.withSubscriptionName(subscription)
				.build();
		System.out.println(pubsubSource.toString());
		
		DataStream<PubSubEvent> stream = streamEnv.addSource(pubsubSource);
		stream.print();
		
		DataStream<Tuple2<String, String>> result = stream.flatMap(new FlatMapFunction<PubSubEvent, Tuple2<String, String>>() {

			private static final long serialVersionUID = -8876908366281948639L;

			@Override
			public void flatMap(PubSubEvent value, Collector<Tuple2<String, String>> out) throws Exception {
				Tuple2<String, String> t = new Tuple2<String, String>();
				System.out.println("mapping");
				t.f0 = value.type;
				t.f1 = value.event;
			}
			
		}
		);
		
		result.print();
		
		streamEnv.execute("Reading pubsub data");
		
	}
}
