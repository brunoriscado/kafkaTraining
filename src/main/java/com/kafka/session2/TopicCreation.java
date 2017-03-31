package com.kafka.session2;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

/**
 * Created by bruno on 31/03/17.
 */
public class TopicCreation {
    static void createTopic(String zookeeper, String topicName, int partition) {
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zookeeper, 300, 3000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeper), false);

            Properties topicConfig = new Properties();

            AdminUtils.createTopic(zkUtils,
                    topicName,
                    partition,
                    1,
                    topicConfig,
                    RackAwareMode.Disabled$.MODULE$);
        } catch (Exception e) {
            //Retry on exception
            System.out.println(e.getMessage());
        } finally {
            zkClient.close();
        }
    }

}
