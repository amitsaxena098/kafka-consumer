package com.kafka.consumer;

import com.kafka.consumer.consume.Consumer;
import com.kafka.consumer.interfaces.IConsumer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		IConsumer consumer = new Consumer();
		consumer.consume();
	}
}
