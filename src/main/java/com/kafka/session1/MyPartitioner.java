package com.kafka.session1;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by bruno on 30/03/17.
 */
public class MyPartitioner implements Partitioner {
    private CustomerService customerService = new CustomerService();

    @Override
    public int partition(String topicName,
            Object key,
            byte[] keyBytes,
            Object value,
            byte[] valueBytes,
            Cluster cluster) {
        int partition = 0;
        String username = (String)key;
        Integer userId = customerService.findUserById(username);

        if (userId != null) {
            partition = userId;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
