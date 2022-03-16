package com.github.grigor.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack implements ConnectionInterface{
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);


    public static void main(String[] args) {

        for (int i = 0; i < 10; i++) {
            //properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostPort);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            //producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            //create record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world!" + i);
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("message gotten : \n" + "topic : " + metadata.topic() +
                                "\n" + "offset : " + metadata.offset() + "\n" + "partition : " +
                                metadata.partition() + "\n" + "time : " + metadata.timestamp());
                    } else {
                        logger.error("error while sending data : " + exception);
                    }
                }
            });

            producer.flush();
            producer.close();
        }
    }
}
