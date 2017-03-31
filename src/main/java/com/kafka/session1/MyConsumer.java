package com.kafka.session1;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by bruno on 30/03/17.
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "customers-group4");
//        properties.put("auto.commit.interval.ms", "1000");
//        properties.put("group.id", "customers-group4");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singleton("my-topic"));



        while(true) {
            ConsumerRecords<String, String> consumerRecord = consumer.poll(500);
            consumerRecord.forEach(record -> {
                System.out.println(record.topic() + " " +
                        record.toString() + " " +
                        record.partition() + " " +
                        record.offset());
            });
            try {
                consumer.commitAsync((map, e) -> {
                    if (e != null) {
                        System.out.println("commit failed");
                    }
                });
            } catch (CommitFailedException e) {
                System.out.println("Commit offset failed: " + e.getMessage());
            }
        }
    }
}
