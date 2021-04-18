package ak.kafka.stream;

import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class CommonConfig {
	@Autowired
	private Environment envProps;
	
	public void createTopics(String topicname,String topicpartitions,String topicreplicationfactor) {
		Map<String, Object> config = new HashMap<>();
		log.info("envProps = {}", envProps);
		config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
		AdminClient client = AdminClient.create(config);

		List<NewTopic> topics = new ArrayList<>();

		topics.add(new NewTopic(topicname,
				parseInt(topicpartitions),
				parseShort(topicreplicationfactor)));

		client.createTopics(topics);
		client.close();
	}
}
