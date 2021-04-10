package ak.kafka.stream;

import org.apache.kafka.common.serialization.Serde;
//import java.util.Properties;
//import java.util.regex.Pattern;
//
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.Materialized;
//import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

import ak.kafka.stream.avro.Color;
import ak.kafka.stream.avro.User;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

@Component
@Slf4j
//@EnableKafkaStreams
public class StreamRunner implements ApplicationRunner {
	@Autowired
	private  AvroProtobufStreamSerde streamSerilization;
	@Autowired
	private AvroColorFilter avroColorFilter;
//	private final static String inputTopic = "inputTopic";
//	private final static String bootstrapServers = "localhost:9092";
//
	@Override
	public void run(ApplicationArguments arg0) throws Exception {
		//transformBasicStream();
		log.info("StreamRunner is invoked ");
		streamSerilization.runTutorial("");
		avroColorFilter.handleStream();
}

	private void transformBasicStream() {
		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> textLines = builder.stream("user-topic");

		textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.groupBy((key, value) -> value).count(Materialized.as("WordCount")).toStream()
				//.to("twitter_twit", Produced.with(Serdes.String(), Serdes.Long()));
				.to("color-topic", Produced.with(String(), Long()));
		final Topology topology = builder.build();

		Properties props = new Properties();
		props.put(APPLICATION_ID_CONFIG, "twitter_twit");
		props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
//		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG,String().getClass());
		final KafkaStreams streams = new KafkaStreams(topology, props);

		final CountDownLatch latch = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}

	
	}

