package com.kafka.session2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by bruno on 31/03/17.
 */
public class WindowJoin {
    public static void main(String[] args) {
        String broker = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "search-join");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //Tasks minimum of all is 3 for this case
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<String, String> searches = kStreamBuilder.stream("searches");
        KStream<String, String> clicks = kStreamBuilder.stream("clicks");

        KStream<String, String> joined = searches.join(clicks,
                (searchValue, clickValue) -> "search is " + searchValue + " click is " + clickValue,
                JoinWindows.of(TimeUnit.SECONDS.toMillis(5)),
                Serdes.String(),
                Serdes.String(),
                Serdes.String());

        joined.to(Serdes.String(), Serdes.String(), "result-topic-searches-clicks");

        KafkaStreams streams = new KafkaStreams(kStreamBuilder, properties);
        streams.start();

    }
}
