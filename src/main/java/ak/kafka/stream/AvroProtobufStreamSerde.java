package ak.kafka.stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import ak.kafka.stream.avro.Movie;
import ak.kafka.stream.avro.MovieProtos;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.extern.slf4j.Slf4j;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

//import io.confluent.developer.avro.Movie;
//import io.confluent.developer.proto.MovieProtos;
//import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
//import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
//import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
//
//import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@Component
public class AvroProtobufStreamSerde {
	@Autowired
	private Environment envProps;
	protected Properties buildStreamsProperties(Environment envProps2) {
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps2.getProperty("application.id"));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps2.getProperty("bootstrap.servers"));
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				envProps2.getProperty("schema.registry.url"));

		return props;
	}

	private void createTopics(Environment envProps2) {
		Map<String, Object> config = new HashMap<>();
		log.info("envProps = {}", envProps2);
		config.put("bootstrap.servers", envProps2.getProperty("bootstrap.servers"));
		AdminClient client = AdminClient.create(config);

		List<NewTopic> topics = new ArrayList<>();

		topics.add(new NewTopic(envProps2.getProperty("input.avro.movies.topic.name"),
				parseInt(envProps2.getProperty("input.avro.movies.topic.partitions")),
				parseShort(envProps2.getProperty("input.avro.movies.topic.replication.factor"))));

		topics.add(new NewTopic(envProps2.getProperty("output.proto.movies.topic.name"),
				parseInt(envProps2.getProperty("output.proto.movies.topic.partitions")),
				parseShort(envProps2.getProperty("output.proto.movies.topic.replication.factor"))));

		client.createTopics(topics);
		client.close();
	}

	protected SpecificAvroSerde<Movie> movieAvroSerde(Environment envProps2) {
		SpecificAvroSerde<Movie> movieAvroSerde = new SpecificAvroSerde<>();

		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps2.getProperty("schema.registry.url"));
		movieAvroSerde.configure(serdeConfig, false);
		return movieAvroSerde;
	}

	protected KafkaProtobufSerde<MovieProtos.Movie> movieProtobufSerde(Environment envProps2) {
		final KafkaProtobufSerde<MovieProtos.Movie> protobufSerde = new KafkaProtobufSerde<>();

		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps2.getProperty("schema.registry.url"));
		protobufSerde.configure(serdeConfig, false);
		return protobufSerde;
	}

	protected Topology buildTopology(Environment envProps2, final SpecificAvroSerde<Movie> movieSpecificAvroSerde,
			final KafkaProtobufSerde<MovieProtos.Movie> movieProtoSerde) {

		final String inputAvroTopicName = envProps2.getProperty("input.avro.movies.topic.name");
		final String outProtoTopicName = envProps2.getProperty("output.proto.movies.topic.name");

		final StreamsBuilder builder = new StreamsBuilder();

		// topic contains values in avro format
		final KStream<Long, Movie> avroMovieStream = builder.stream(inputAvroTopicName,
				Consumed.with(Long(), movieSpecificAvroSerde));
		
		avroMovieStream.foreach(new ForeachAction<Long, Movie>() {
		    public void apply(Long key, Movie value) {
		        log.info(key + ": " + value);
		    }
		 });
		// convert and write movie data in protobuf format
		avroMovieStream
				.map((key, avroMovie) -> new KeyValue<>(key,
						MovieProtos.Movie.newBuilder().setMovieId(avroMovie.getMovieId()).setTitle(avroMovie.getTitle())
								.setReleaseYear(avroMovie.getReleaseYear()).build()))
				.to(outProtoTopicName, Produced.with(Long(), movieProtoSerde));

		return builder.build();
	}

	protected Properties loadEnvProperties(String fileName) throws IOException {
		Properties envProps = new Properties();
		log.info("envProps = {}", envProps);
		FileInputStream input = new FileInputStream(fileName);
		log.info("input = {}", input);
		envProps.load(input);
		input.close();
		return envProps;
	}

	protected void runTutorial(String configPath) throws IOException {
        log.info("runTutorial invoked in stream runner");
		//Properties envProps = this.loadEnvProperties(configPath);
		Properties streamProps = this.buildStreamsProperties(envProps);
		log.info("envProps invoked in stream runner :: {}",envProps);
		log.info("streamProps invoked in stream runner :: {}",streamProps);
		Topology topology = this.buildTopology(envProps, this.movieAvroSerde(envProps),
				this.movieProtobufSerde(envProps));
		this.createTopics(envProps);

		final KafkaStreams streams = new KafkaStreams(topology, streamProps);
		final CountDownLatch latch = new CountDownLatch(1);

		// Attach shutdown handler to catch Control-C.
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close(Duration.ofSeconds(5));
				latch.countDown();
			}
		});

		try {
			streams.cleanUp();
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}

//	public static void main(String[] args) throws IOException {
//		if (args.length < 1) {
//			throw new IllegalArgumentException(
//					"This program takes one argument: the path to an environment configuration file.");
//		}
//
//	}
}