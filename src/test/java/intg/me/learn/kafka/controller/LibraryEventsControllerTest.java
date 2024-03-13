package me.learn.kafka.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import me.learn.kafka.controller.util.TestUtil;
import me.learn.kafka.domain.LibraryEvent;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventsControllerTest {

	@Autowired
	TestRestTemplate restTemplate;

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
		
	}

}