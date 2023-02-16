package com.kafka.jpa;

import org.springframework.data.repository.CrudRepository;

import com.kafka.entity.LibraryEvent;

public interface LibraryEventsRepository
	extends CrudRepository<LibraryEvent, Integer> {

}
