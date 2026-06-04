package com.kafka.consumer.consume;

import com.kafka.consumer.config.ConsumerConfig;
import com.kafka.consumer.interfaces.IConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerWithShutdown implements IConsumer {


    @Override
    public void consume() {
        String groupId = "my-consumer";
        String topic = "testTopic";

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET, "earliest");
        kafkaProperties.put(ConsumerConfig.GROUP_ID, groupId);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown...triggering consumer.wakeup()");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            kafkaConsumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(2000));
                for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Key: {}, Value: {}, Partition: {}", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition());
                }
            }
        } catch (WakeupException e) {
            log.info("WakeUpException thrown.....");
        } catch (Exception e) {
            log.error("Exception occurred : {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
            log.info("Consumer is gracefully shutdown");
        }
    }
}
