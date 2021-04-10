package ak.kafka.stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import static org.apache.kafka.common.serialization.Serdes.String;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
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
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@EnableKafkaStreams
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

		client.createTopics(topics);
		client.close();
	}

	
	public KStream<String, User> handleStream() throws JsonProcessingException {
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, User> userStream = builder.stream("avro-users",Consumed.with(String(), userAvroSerde()));
		userStream.foreach(new ForeachAction<String, User>() {
			public void apply(String key, User value) {
				log.info(key + ": " + value);
			}
		});
		KStream<String, Color> colorStream = userStream
				.filter((username, user) -> !"blue".equals(user.getFavoriteColor()))
				.mapValues((username, user) -> new Color(user.getFavoriteColor()));
		
		colorStream.to("avro-colors",Produced.with(String(), colorAvroSerde()));
		
		return userStream;
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
}
