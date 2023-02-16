package com.kafka.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.domain.LibraryEvent;
import com.kafka.domain.LibraryEventType;
import com.kafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
	    @Valid @RequestBody LibraryEvent libraryEvent)
	    throws JsonProcessingException {

	// SendResult<Integer, String> sendResult =
	libraryEvent.setLibraryEventType(LibraryEventType.NEW);
	libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
	// log.info("Send Result is {}", sendResult.toString());

	return ResponseEntity.status(HttpStatus.CREATED)
		.body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(
	    @Valid @RequestBody LibraryEvent libraryEvent)
	    throws JsonProcessingException {

	if (libraryEvent.getLibraryEventId() == null) {
	    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
		    .body("Please pass the LibraryEventId");
	}

	libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
	libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

	return ResponseEntity.status(HttpStatus.OK)
		.body(libraryEvent);
    }
}
