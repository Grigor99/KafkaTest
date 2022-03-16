package com.github.grigor.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo implements ConnectionInterface {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);


        //create consumer config
        String groupId = "my-fourth-application";
        String readFromTheBeginning = "earliest";
        String readOnlyNewMessages = "latest";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostPort);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, readFromTheBeginning);
        //create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);
        //subscribe consumer to the topic
        consumer.subscribe(Arrays.asList(topic));
        //poll
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("topic: " + record.topic() + "\n" +
                        "key: " + record.key() + "\n" +
                        "value: " + record.value() + "\n" +
                        "partition: " + record.partition());
            }
        }


    }
}
