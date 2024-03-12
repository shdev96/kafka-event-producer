package me.learn.kafka.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.learn.kafka.domain.LibraryEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventsProducer {

	@Value("${spring.kafka.topic}")
	public String topic;

	private final KafkaTemplate<Integer, String> kafkaTemplate;
	private final ObjectMapper objectMapper;

	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

		Integer key = libraryEvent.libraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.sendDefault(key, value);
		return completableFuture
			.whenComplete((sendResult, throwable) -> {
				if (throwable != null) {
					handleFailure(key, value, throwable);
				} else {
					handleSuccess(key, value, sendResult);

				}
			});
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

		Integer key = libraryEvent.libraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("Exception Sending the Message and the exception is {}", e.getMessage());
			throw e;
		}

		return sendResult;

	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error Sending the Message and the exception is {}", ex.getMessage());
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
	}
}
