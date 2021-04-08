package ak.kafka.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import ak.kafka.stream.avro.Color;
import ak.kafka.stream.avro.User;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class TopologyTestDriverAvroApplicationTests {

	  private static final String SCHEMA_REGISTRY_SCOPE = TopologyTestDriverAvroApplicationTests.class.getName();
	  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

	  private TopologyTestDriver testDriver;

	  private TestInputTopic<String, User> usersTopic;
	  private TestOutputTopic<String, Color> colorsTopic;

	  @BeforeEach
	  void beforeEach() throws Exception {
	    // Create topology to handle stream of users
	    StreamsBuilder builder = new StreamsBuilder();
	    new KafkaStreamApplication().handleStream(builder);
	    Topology topology = builder.build();

	    // Dummy properties needed for test diver
	    Properties props = new Properties();
	    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
	    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
	    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
	    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
	    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

	    // Create test driver
	    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

	    // Create Serdes used for test record keys and values
	    Serde<String> stringSerde = Serdes.String();
	    Serde<User> avroUserSerde = new SpecificAvroSerde<>();
	    Serde<Color> avroColorSerde = new SpecificAvroSerde<>();

	    // Configure Serdes to use the same mock schema registry URL
//	    Map<String, String> config = Map.of(
//	        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
	    Map<String, String> serdeConfig = new HashMap<>();
	    serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
	    avroUserSerde.configure(serdeConfig, false);
	    avroColorSerde.configure(serdeConfig, false);

	    // Define input and output topics to use in tests
	    usersTopic = testDriver.createInputTopic(
	        "user-topic",
	        stringSerde.serializer(),
	        avroUserSerde.serializer());
	    colorsTopic = testDriver.createOutputTopic(
	        "color-topic",
	        stringSerde.deserializer(),
	        avroColorSerde.deserializer());
	  }

	  @AfterEach
	  void afterEach() {
//	    testDriver.close();
//	    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
	  }

	  @Test
	  void shouldPropagateUserWithFavoriteColorRed() throws Exception {
	    User user = new User("Alice", 7, "red");
	   
	    usersTopic.pipeInput("Alice", user);
	    assertEquals(new Color("red"), colorsTopic.readValue());
	  }

//	  @Test
//	  void shouldNotPropagateUserWithFavoriteColorBlue() throws Exception {
//	    User user = new User("Bob", 14, "blue");
//	    usersTopic.pipeInput("Bob", user);
//	    assertTrue(colorsTopic.isEmpty());
//	  }

	}
