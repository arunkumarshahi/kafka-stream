package ak.kafka.stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.common.serialization.Serdes.String;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
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

import ak.kafka.stream.avro.Color;
import ak.kafka.stream.avro.Movie;
//import ak.kafka.stream.avro.MovieProtos;
import ak.kafka.stream.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
//@EnableKafkaStreams
@Slf4j
public class KafkaStreamApplication {
	@Autowired
	private Environment envProps;
	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamApplication.class, args);
	}

//	@Bean
//	public KStream<String, User> handleStream(StreamsBuilder builder) throws JsonProcessingException {
//		KStream<String, User> userStream = builder.stream("avro-users",Consumed.with(String(), userAvroSerde()));
//		userStream.foreach(new ForeachAction<String, User>() {
//			public void apply(String key, User value) {
//				log.info(key + ": " + value);
//			}
//		});
//		KStream<String, Color> colorStream = userStream
//				.filter((username, user) -> !"blue".equals(user.getFavoriteColor()))
//				.mapValues((username, user) -> new Color(user.getFavoriteColor()));
//		
//		colorStream.to("avro-colors",Produced.with(String(), colorAvroSerde()));
//		colorStream.foreach(new ForeachAction<String, Color>() {
//			public void apply(String key, Color value) {
//				log.info("filtered color  = " + key + ": " + value);
//			}
//		});
//		return userStream;
//	}
//	
//	protected SpecificAvroSerde<User> userAvroSerde() {
//		SpecificAvroSerde<User> movieAvroSerde = new SpecificAvroSerde<>();
//
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
//		movieAvroSerde.configure(serdeConfig, false);
//		return movieAvroSerde;
//	}
//
//	protected SpecificAvroSerde<Color> colorAvroSerde() {
//		SpecificAvroSerde<Color> movieAvroSerde = new SpecificAvroSerde<>();
//
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
//		movieAvroSerde.configure(serdeConfig, false);
//		return movieAvroSerde;
//	}
}
