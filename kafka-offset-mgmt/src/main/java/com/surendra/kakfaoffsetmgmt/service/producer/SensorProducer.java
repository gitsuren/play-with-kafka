package com.surendra.kakfaoffsetmgmt.service.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;


@Configuration
public class SensorProducer {

    private static final Logger log = LoggerFactory.getLogger(SensorProducer.class);

    private String topicName = "SensorTopic";

    public void produce(String message){
        log.info("============sending message to topic:  " + topicName +"||" + message);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "com.surendra.kakfaoffsetmgmt.partitioner.SensorPartitioner");
        properties.put("speed.sensor.name", "TSS");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName, "SSP" + i, message + i));

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName, "TSS", message + i));

        producer.close();
        System.out.println("SimpleProducer Completed.");
    }


}
