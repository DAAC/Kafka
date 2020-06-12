package com.mx.daac.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    Properties properties = new Properties();
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


    public ConsumerThread(CountDownLatch latch,
                          String bootstrapServers,
                          String groupId,
                          String topic) {
        this.latch = latch;

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest, latest, none


        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    logger.info("\n" + "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n");
                }

            }
        } catch (WakeupException wakeupException) {
            logger.info("Received shutdown signal");
        }finally {
            consumer.close();
            //tell our main coe we are done with the consumer
            latch.countDown();
        }
    }


    public void shutDown() {
        //the wakeUp method is a special method to interrup consumer.poll method
        //it will throw the exception WakeUpException
        consumer.wakeup();

    }

}
