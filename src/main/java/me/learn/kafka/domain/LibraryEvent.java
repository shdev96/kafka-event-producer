package me.learn.kafka.domain;

public record LibraryEvent(Integer libraryEventId, LibraryEventType libraryEventType, Book book) {

}
