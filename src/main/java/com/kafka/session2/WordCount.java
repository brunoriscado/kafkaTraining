package com.kafka.session2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by bruno on 31/03/17.
 * https://github.com/confluentinc/examples/tree/3.2.x/kafka-streams
 */
public class WordCount {
    public static void main(String[] args) {
        String broker = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //Tasks minimum of all is 3 for this case
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> words = builder.stream("words");

        KTable<String, String> count = words
                .flatMapValues(entry -> Arrays.asList(entry.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count("WordCount")
                .mapValues(value -> value.toString());

        count.print();

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();
    }
}
