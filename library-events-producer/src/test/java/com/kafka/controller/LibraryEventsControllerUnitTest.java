package com.kafka.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.Book;
import com.kafka.domain.LibraryEvent;
import com.kafka.producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer libraryEventProducer;

    @Test
    void postLibraryEvent() throws Exception {
	// given
	Book book = Book.builder().bookId(123).bookAuthor("OkayChamp")
		.bookName("Kafka using Spring Boot").build();

	LibraryEvent libraryEvent = LibraryEvent.builder()
		.libraryEventId(null).book(book).build();

	String json = objectMapper.writeValueAsString(libraryEvent);

	doNothing().when(libraryEventProducer)
		.sendLibraryEvent_Approach2(isA(LibraryEvent.class));
	// when
	mockMvc.perform(post("/v1/libraryevent").content(json)
		.contentType(MediaType.APPLICATION_JSON))
		.andExpect(status().isCreated());
	// then
    }
    
    @Test
    void postLibraryEvent_4xx() throws Exception {
	// given

	LibraryEvent libraryEvent = LibraryEvent.builder()
		.libraryEventId(null).book(null).build();

	String json = objectMapper.writeValueAsString(libraryEvent);

	// when
	mockMvc.perform(post("/v1/libraryevent").content(json)
		.contentType(MediaType.APPLICATION_JSON))
		.andExpect(status().is4xxClientError());
	// then
    }

}
