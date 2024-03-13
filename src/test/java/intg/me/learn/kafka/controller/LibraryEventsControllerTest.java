package me.learn.kafka.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;

import me.learn.kafka.controller.util.TestUtil;
import me.learn.kafka.domain.LibraryEvent;

@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
	"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsControllerTest {

	@Autowired
	TestRestTemplate restTemplate;

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	ObjectMapper objectMapper;

	private Consumer<Integer, String> consumer;

	@BeforeEach
	void setUp() {

		var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();

		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);


	}

	@AfterEach
	void tearDown(){

		consumer.close();
	}

	@Test
	void postLibraryEvent() throws Exception {

		// given
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
		var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(), httpHeaders);

		// when
		var responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, httpEntity, LibraryEvent.class);

		//then
		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

		ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
		;
		assert consumerRecords.count() == 1;
		consumerRecords.forEach(record -> {
			var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
			assertEquals(libraryEventActual, TestUtil.libraryEventRecord());

		});
	}

}