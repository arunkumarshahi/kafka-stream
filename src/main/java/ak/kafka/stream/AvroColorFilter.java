package ak.kafka.stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import static org.apache.kafka.common.serialization.Serdes.String;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;

import ak.kafka.stream.avro.Color;
import ak.kafka.stream.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Slf4j
@Component
//@EnableKafkaStreams
public class AvroColorFilter {
	@Autowired
	private Environment envProps;

	@Bean
	private void createTopics() {
		Map<String, Object> config = new HashMap<>();
		log.info("envProps = {}", envProps);
		config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
		AdminClient client = AdminClient.create(config);

		List<NewTopic> topics = new ArrayList<>();

		topics.add(new NewTopic(envProps.getProperty("input.avro.users.topic.name"),
				parseInt(envProps.getProperty("input.avro.users.topic.partitions")),
				parseShort(envProps.getProperty("input.avro.users.topic.replication.factor"))));

		topics.add(new NewTopic(envProps.getProperty("input.avro.colors.topic.name"),
				parseInt(envProps.getProperty("input.avro.colors.topic.partitions")),
				parseShort(envProps.getProperty("input.avro.colors.topic.replication.factor"))));

		topics.add(new NewTopic(envProps.getProperty("input.avro.sum.topic.name"),
				parseInt(envProps.getProperty("input.avro.colors.topic.partitions")),
				parseShort(envProps.getProperty("input.avro.colors.topic.replication.factor"))));

		client.createTopics(topics);
		client.close();
	}

	public Topology handleStream() throws JsonProcessingException {
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, User> userStream = builder.stream("avro-users", Consumed.with(String(), userAvroSerde()));
		log.info("sum stream   called ");
		// adding aggregation of amount for a key
		KStream<String, Integer> sumStream=userStream.map((k, v) -> new KeyValue<>((String)v.getName(), (Integer)v.getFavoriteNumber()))
				// Group by title
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
				// Apply SUM aggregation
				.reduce(Integer::sum)
				// Write to stream specified by outputTopic
				.toStream();
				
		
		sumStream.foreach(new ForeachAction<String, Integer>() {
			public void apply(String key, Integer value) {
				log.info("sum stream   = " + key + ": " + value);
			}
		});
		sumStream.to(envProps.getProperty("input.avro.sum.topic.name"),
				Produced.with(Serdes.String(), Serdes.Integer()));
		// end of aggregation
		userStream.foreach(new ForeachAction<String, User>() {
			public void apply(String key, User value) {
				log.info(key + ": " + value);
			}
		});
		KStream<String, Color> colorStream = userStream
				.filter((username, user) -> !"blue".equals(user.getFavoriteColor()))
				.mapValues((username, user) -> new Color(user.getFavoriteColor()));
		colorStream.foreach(new ForeachAction<String, Color>() {
			public void apply(String key, Color value) {
				log.info("filtered color  = " + key + ": " + value);
			}
		});
		colorStream.to("avro-colors", Produced.with(String(), colorAvroSerde()));
		// final KafkaStreams streams = new KafkaStreams(topology, props);
		final Topology topology = builder.build();
		return topology;
	}

	protected SpecificAvroSerde<User> userAvroSerde() {
		SpecificAvroSerde<User> movieAvroSerde = new SpecificAvroSerde<>();

		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
		movieAvroSerde.configure(serdeConfig, false);
		return movieAvroSerde;
	}

	protected SpecificAvroSerde<Color> colorAvroSerde() {
		SpecificAvroSerde<Color> movieAvroSerde = new SpecificAvroSerde<>();

		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
		movieAvroSerde.configure(serdeConfig, false);
		return movieAvroSerde;
	}

	protected Properties buildStreamsProperties() {
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				envProps.getProperty("schema.registry.url"));

		return props;
	}

	public void avoroUserProducer() {
		log.info("avoroUserProducer job started");
		Properties streamProps = this.buildStreamsProperties();
		streamProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class);
		streamProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		log.info("avoroUserProducer is called ");
		String keys[] = new String[] { "arun", "julie", "Sanvi", "Shravya" };
		KafkaProducer producer = new KafkaProducer(streamProps);
		while (true) {
			try {
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			for (String key : keys) {
				User user = new User();
				user.setName(key);
				Random random = new Random();
				int value = random.nextInt(900) + 100;
				user.setFavoriteNumber(value);
				user.setFavoriteColor("BLACK");
				log.info("avoroUserProducer is sending data  ={}", user);

				ProducerRecord<String, User> record = new ProducerRecord<>(
						envProps.getProperty("input.avro.users.topic.name"), key, user);
				try {
					producer.send(record);
				} catch (SerializationException e) {
					// may need to do something with it
				}

			}
		}

	}

}
