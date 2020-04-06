package com.mx.daac.kafka.tutorialone;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch latch, String topic, Properties properties){
        this.latch = latch;
        this.consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    logger.info("key: " + record.key() + " value: " + record.value());
                    logger.info("partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException wakeUpException){
            logger.info("Received shutdown signal!");
        }finally {
            consumer.close();
            //tell our main code we are done with the consumer
            latch.countDown();
        }
    }

    public void shotDown(){
        //method is a special method to interupt poll
        consumer.wakeup();
    }
}
