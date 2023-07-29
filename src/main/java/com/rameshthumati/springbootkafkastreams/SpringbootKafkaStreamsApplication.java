package com.rameshthumati.springbootkafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
@SpringBootApplication
//topic.metadata.results
//topic-inference-results
//topic-metadata-inference-join
@EnableKafka
@EnableKafkaStreams
public class SpringbootKafkaStreamsApplication {
	public static void main(String[] args) {
		SpringApplication.run(SpringbootKafkaStreamsApplication.class, args);
	}

}
