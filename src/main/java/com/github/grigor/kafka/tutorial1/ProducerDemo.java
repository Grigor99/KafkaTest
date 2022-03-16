package com.github.grigor.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo implements ConnectionInterface {

    public static void main(String[] args) {

        //properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostPort);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //create record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world!");
        //send data
        producer.send(record);

        producer.flush();
        producer.close();
    }
}
