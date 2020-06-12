package com.mx.daac.simplesteph.kafka.tutorial1;

import jdk.nashorn.internal.ir.WhileNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        String groudId = "my-fourth-application";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groudId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest, latest, none

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //two ways to subscribe a topic or topics
        consumer.subscribe(Collections.singleton(topic));
        //consumer.subscribe(Arrays.asList("first_topic", "second_topic","third_topic"));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records){
                logger.info("\n"+"Key: " + record.key()+"\n"+
                        "Value: "+ record.value()+"\n"+
                        "Partition: " + record.partition() +"\n"+
                        "Offset: "+ record.offset()+"\n");
            }

        }


    }

}
