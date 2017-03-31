package com.kafka.session2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * Created by bruno on 31/03/17.
 */
public class Words {
    public static void main(String[] args) {
        TopicCreation.createTopic("127.0.0.1:2181", "words", 3);

        String brokers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        Random r = new Random();

        while(true) {
            Integer randomKey = r.nextInt(10);
            ProducerRecord<String, String> record = new ProducerRecord("words", String.valueOf(r.nextInt(3)), "this is a sentence");
            producer.send(record);
        }
    }
}
