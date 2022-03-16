package com.github.grigor.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignSeek implements ConnectionInterface {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);


        //create consumer config

        String readFromTheBeginning = "earliest";
        String readOnlyNewMessages = "latest";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostPort);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, readFromTheBeginning);
        //create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);


        //assign
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(List.of(partition));
        //seek
        long offsetFromRead = 15L;
        consumer.seek(partition, offsetFromRead);


        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;
        //poll
        while (keepOnReading) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("topic: " + record.topic() + "\n" +
                        "key: " + record.key() + "\n" +
                        "offset: " + record.offset() + "\n" +
                        "value: " + record.value() + "\n" +
                        "partition: " + record.partition());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }

            }
        }
        logger.info("exiting the application");


    }
}
