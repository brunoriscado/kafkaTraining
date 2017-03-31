package com.kafka.session2;

/**
 * Created by bruno on 31/03/17.
 */
public class MyExample {

    public static void main(String[] args) {
        TopicCreation.createTopic("127.0.0.1:2181", "topicA", 3);
    }
}
