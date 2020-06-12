package com.mx.daac.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public ConsumerDemoWithThread(){}

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-fourth-application";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable myConsumerThread =
                new ConsumerThread(latch,
                                   bootstrapServers,
                                   groupId,
                                   topic);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("App has exited");
        }

        ));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("App got interrrupted", e);
        }finally {
            logger.info("App is closing");
        }
    }


    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

}
