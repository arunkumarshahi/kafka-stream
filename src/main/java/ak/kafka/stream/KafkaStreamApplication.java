package ak.kafka.stream;


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import ak.kafka.stream.avro.Color;
import ak.kafka.stream.avro.User;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
//@EnableKafkaStreams
@Slf4j
public class KafkaStreamApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamApplication.class, args);
//		 User user = new User("Alice", 7, "red");
		   
		    //usersTopic.pipeInput("Alice", user);
	}

//	@Bean
	public KStream<String, User> handleStream(StreamsBuilder builder) throws JsonProcessingException {

		KStream<String, User> userStream = builder.stream("user-topic");
		KStream<String, Color> colorStream = userStream
				.filter((username, user) -> !"blue".equals(user.getFavoriteColor()))
				.mapValues((username, user) -> new Color(user.getFavoriteColor()));
		colorStream.to("color-topic");
		return userStream;
	}
}
