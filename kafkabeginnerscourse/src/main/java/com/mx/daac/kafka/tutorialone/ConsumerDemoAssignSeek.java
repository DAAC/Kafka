package com.mx.daac.kafka.tutorialone;

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
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));
        //seek
        consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        int numberOfMsgsToRead = 5;
        boolean keepOnReading = true;
        int numberOfMsgsReadFar = 0;

        while (keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records){
                numberOfMsgsReadFar +=1;
                logger.info("key: " + record.key() + " value: " + record.value());
                logger.info("partition: " + record.partition() + " Offset: " + record.offset());
                if (numberOfMsgsReadFar >= numberOfMsgsToRead){
                    keepOnReading = false; //exit the while loop
                    break; // exit the for loop
                }
            }
        }
        logger.info("Exiting the app");
    }

}
