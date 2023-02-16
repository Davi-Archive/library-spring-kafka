package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.domain.LibraryEvent;
import com.kafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@RestController
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
	    @RequestBody LibraryEvent libraryEvent)
	    throws JsonProcessingException {

	SendResult<Integer, String> sendResult = libraryEventProducer
		.sendLibraryEventSynchronous(libraryEvent);
	log.info("Send Result is {}", sendResult.toString());

	return ResponseEntity.status(HttpStatus.CREATED)
		.body(libraryEvent);
    }
}
