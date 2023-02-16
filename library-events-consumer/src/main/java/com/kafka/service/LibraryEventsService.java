package com.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;

    @KafkaListener(topics = { "library-events" })
    public void processLibraryEvent(
	    ConsumerRecord<Integer, String> consumerRecord)
	    throws JsonMappingException, JsonProcessingException {
	LibraryEvent libraryEvent = objectMapper.readValue(
		consumerRecord.value(), LibraryEvent.class);
	log.info("libraryEvent : {}", libraryEvent);
    }

}
