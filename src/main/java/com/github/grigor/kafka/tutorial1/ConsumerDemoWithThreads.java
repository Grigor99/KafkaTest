package com.github.grigor.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads implements ConnectionInterface {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {

    }

    public void run() {
        String groupId = "my-fifth-application";
        String readFromTheBeginning = "earliest";
        String readOnlyNewMessages = "latest";
        String topic = "first_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable runnable = new ConsumerThread(countDownLatch, topic, groupId, readFromTheBeginning);
        Thread thread = new Thread(runnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shut down hook");
            ((ConsumerThread) runnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.info("got interrupted: " + e);
        } finally {
            logger.info("closing the consumer");
        }
    }

    public class ConsumerThread implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch countDownLatch, String topic, String groupId, String readFrom) {
            this.countDownLatch = countDownLatch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostPort);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, readFrom);
            this.consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {

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
            } catch (WakeupException e) {
                logger.error("shut up exception :" + e);
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            //interrupts the poll
            consumer.wakeup();
        }
    }
}
