package com.kafka.producer;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent)
	    throws JsonProcessingException {

	Integer key = libraryEvent.getLibraryEventId();
	String value = objectMapper.writeValueAsString(libraryEvent);

	ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate
		.sendDefault(key, value);
	listenableFuture.addCallback(
		new ListenableFutureCallback<SendResult<Integer, String>>() {

		    @Override
		    public void onSuccess(
			    SendResult<Integer, String> result) {

			handleSuccess(key, value, result);
		    }

		    @Override
		    public void onFailure(Throwable ex) {

			handleFailure(key, value, ex);
		    }
		});
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(
	    LibraryEvent libraryEvent)
	    throws JsonProcessingException {

	Integer key = libraryEvent.getLibraryEventId();
	String value = objectMapper.writeValueAsString(libraryEvent);
	SendResult<Integer, String> sendResult = null;
	try {
	    sendResult = kafkaTemplate.sendDefault(key, value).get();
	} catch (ExecutionException | InterruptedException e) {
	    log.error(
		    "Execution/Interrupted exception sending the message, exception: {}",
		    e.getMessage());
	    e.printStackTrace();
	} catch (Exception e) {
	    log.error("Exception sending the message, exception: {}",
		    e.getMessage());
	    e.printStackTrace();
	}
	return sendResult;

    }

    private void handleSuccess(Integer key, String value,
	    SendResult<Integer, String> result) {
	log.info(
		"Message sent Successfully for the key: {} and the value is {}, partition is {}",
		key, value, result.getRecordMetadata().partition());

    }

    private void handleFailure(Integer key, String value,
	    Throwable ex) {

	log.error("Error Sending the message and the exception is {}",
		ex.getMessage());
    }
}
