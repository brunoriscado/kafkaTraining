package com.kafka.session1;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by bruno on 30/03/17.
 */
public class MyProducer {
    public static void main(String[] args) {
        CustomerService customerService = new CustomerService();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "com.kafka.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        customerService.findAllUsers().forEach(user -> {
            try {
                producer.send(new ProducerRecord<String, String>("my-topic", user, "hello-a2")).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        producer.flush();

    }

}
