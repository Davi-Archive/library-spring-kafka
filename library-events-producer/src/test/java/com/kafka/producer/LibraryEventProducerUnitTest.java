package com.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.Book;
import com.kafka.domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @InjectMocks
    LibraryEventProducer eventProducer;

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void sendLibraryEvent_Approach2_failure()
	    throws JsonProcessingException {
	// given
	Book book = Book.builder().bookId(123).bookAuthor("OkayChamp")
		.bookName("Kafka using Spring Boot").build();

	LibraryEvent libraryEvent = LibraryEvent.builder()
		.libraryEventId(null).book(null).build();

	SettableListenableFuture<Object> future = new SettableListenableFuture<>();
	future.setException(
		new RuntimeException("Exception Calling Kafka"));

	when(kafkaTemplate.send(isA(ProducerRecord.class)))
		.thenReturn(future);
	// when
	assertThrows(Exception.class, () -> eventProducer
		.sendLibraryEvent_Approach2(libraryEvent));
	// then
    }

    @Test
    void sendLibraryEvent_Approach2_success()
	    throws JsonProcessingException {
	// given
	Book book = Book.builder().bookId(123).bookAuthor("OkayChamp")
		.bookName("Kafka using Spring Boot").build();

	LibraryEvent libraryEvent = LibraryEvent.builder()
		.libraryEventId(null).book(null).build();

	String record = objectMapper.writeValueAsString(libraryEvent);

	SettableListenableFuture<Object> future = new SettableListenableFuture<>();

	ProducerRecord<Integer, Field.Str> producerRecord = new ProducerRecord(
		"library-events", libraryEvent.getLibraryEventId(),
		record);
	RecordMetadata recordMetadata = new RecordMetadata(
		new TopicPartition("library-events", 1), 1, 1, 342,
		System.currentTimeMillis(), 1, 2);
	future.set(future);

	when(kafkaTemplate.send(isA(ProducerRecord.class)))
		.thenReturn(future);
	// when
	assertThrows(Exception.class, () -> eventProducer
		.sendLibraryEvent_Approach2(libraryEvent));
	// then
    }

}
