package com.kafka.session2;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by bruno on 31/03/17.
 */
public class Avros {
    public static void main(String[] args) {
        TopicCreation.createTopic("127.0.0.1:2181", "avro", 3);

        String brokers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        properties.put("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<GenericRecord, GenericRecord>(properties);

        Random r = new Random();

        String KEY_SCHEMA = "{`type`:`record`,`name`:`com.saleKey`," +
                "`doc`:`Partition by itemID`," +
                "`fields`:[" +
                "{`name`:`itemID`,`type`:`long`,`doc`:`unique item ID`}" +
                "]}";

        String VALUE_SCHEMA = "{`type`:`record`,`name`:`com.saleValue`," +
                "`doc`:`Partition by itemID`," +
                "`fields`:[" +
                "{`name`:`itemID`,`type`:`long`,`doc`:`unique item ID`}," +
                "{`name`:`items`,`type`:`long`,`doc`:`unique item ID`}" +
                "]}";

        while(true) {
            Integer randomKey = r.nextInt(10);
            Long randomLongKey = r.nextLong();

            GenericRecord key = new GenericData.Record(getSchema(KEY_SCHEMA));
            key.put("itemID", randomLongKey);

            GenericRecord value = new GenericData.Record(getSchema(VALUE_SCHEMA));
            value.put("itemID", randomLongKey);
            value.put("items", 3L);

            ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord("avro", key, value);
            producer.send(record);
        }
    }

    private static Schema getSchema(String schema) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schema.replace('`', '"'));
    }
}
