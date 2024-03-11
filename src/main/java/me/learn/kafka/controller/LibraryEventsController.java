package me.learn.kafka.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.learn.kafka.domain.LibraryEvent;
import me.learn.kafka.producer.LibraryEventsProducer;

@Slf4j
@RestController
@RequiredArgsConstructor
public class LibraryEventsController {

	private final LibraryEventsProducer libraryEventsProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

		//invoke the kafka producer
		log.info("libraryEvent : {}", libraryEvent);
		libraryEventsProducer.sendLibraryEvent(libraryEvent);

		return ResponseEntity.status(HttpStatus.CREATED)
							 .body(libraryEvent);
	}

}
