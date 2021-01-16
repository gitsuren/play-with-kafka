package com.surendra.kakfaoffsetmgmt.service.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.toList;

@Service
public class SensorConsumerWithAutoCommitTrue {

    String topicName = "SensorTopic";

    Properties properties = null;
    private int rCount;

    @PostConstruct
    public void init() {
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "true");
    }

    public void simpleConsumingMessage() {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-simpleConsumingMessages");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topicName));

        try{
            do{
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println(String.format("Records Polled: %s", records.count()));

                rCount = records.count();
                for(ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("offset=%d, key=%s, value=%s", record.offset(), record.key(), record.value()));
                }
            }
            while (rCount>0);
        }
        catch (Exception e){
            System.out.println("Exception in simpleConsumingMessage.. " + e);
        }
        finally {
            consumer.close();
        }

    }

    //https://blog.sysco.no/integration/kafka-rewind-consumers-offset/
    //REPLAY/RETRY/REWIND

    //MIMICING REPLAY/REWIND TO EARLIEST (from the beginning)
    public void replayMessageToBeginning(String topicName, int partition){
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-replayMessageToBeginning");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topicName));

        boolean flag = true;
        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        try{
            do{
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println(String.format("Records Polled: %s", records.count()));

                rCount = records.count();

                if(flag){
                    consumer.seekToBeginning(Stream.of(topicPartition).collect(toList()));
                    flag = false;
                }

                for(ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("offset=%d, key=%s, value=%s", record.offset(), record.key(), record.value()));
                }
            }
            while (rCount>0);
        }
        catch (Exception e){
            System.out.println("Exception in replayMessageToBeginning.. " + e);
        }
        finally {
            consumer.close();
        }
    }

    //MIMICING REPLAY/REWIND FROM SPECIFIC OFFSET
    public void replayMessageFromSpecificOffset(String topicName, int partition, int offset){
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-replayMessageFromSpecificOffset");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topicName));

        boolean flag = true;
        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        try{
            do{
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println(String.format("Records Polled: %s", records.count()));
                System.out.println(String.format("Current position p0: %s", consumer.position(topicPartition)));

                rCount = records.count();

                if(flag){
                    consumer.seek(topicPartition, offset);
                    flag = false;
                    System.out.println(String.format("New Position p0: %s", consumer.position(topicPartition)));
                }

                for(ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("offset=%d, key=%s, value=%s", record.offset(), record.key(), record.value()));
                }
            }
            while (rCount>0);
        }
        catch (Exception e){
            System.out.println("Exception in replayMessageFromSpecificOffset.. " + e);
        }
        finally {
            consumer.close();
        }
    }

    //MIMICING REPLAY/REWIND FROM TIMESTAMP if no offset info
    public void replayMessageByTimeStamp(String topicName, int partition, int timeInMinutes){
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-replayMessageByTimeStamp");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topicName));

        boolean flag = true;
        try{
            do{
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println(String.format("Records Polled: %s", records.count()));

                rCount = records.count();

                if(flag){
                    Map<TopicPartition, Long> query = new HashMap<>();
                        query.put(new TopicPartition(topicName, partition),
                                Instant.now().minus(timeInMinutes, MINUTES).toEpochMilli());
                    Map<TopicPartition, OffsetAndTimestamp> results = consumer.offsetsForTimes(query);

                    results.entrySet()
                            .stream()
                            .forEach(entry -> consumer.seek(entry.getKey(), entry.getValue().offset()));

                    flag = false;
                }

                for(ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("offset=%d, key=%s, value=%s", record.offset(), record.key(), record.value()));
                }
            }
            while (rCount>0);
        }
        catch (Exception e){
            System.out.println("Exception in replayMessageByTimeStamp.. " + e);
        }
        finally {
            consumer.close();
        }
    }

//    public void replayMessage(String topicName, int partition, int offset) {
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor.consumer.false.auto.commit.group.2");
//        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");//to make sure that I only get exactly one desired message on that offset
//
//        TopicPartition topicPartition = new TopicPartition(topicName, partition);
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
//
//        consumer.seek(topicPartition, offset);
//        System.out.println("Current Position tp = " + consumer.position(topicPartition));
//
//        System.out.println("Start Fetching NOW");
//        int counter = 1;// only get 1 message. double guards for max.poll.record
//        int retry = 5; // retry 5 times then give up if cannot receive message
//
//        while (counter > 0) {
//            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
//            if (retry > 0 || !consumerRecords.isEmpty()) {
//                for (ConsumerRecord<String, String> record : consumerRecords) {
//                    counter--;
//                    System.out.println(String.format("==========thread name: %s, partition: %s , offset:%s, key: %s, value: %s", Thread.currentThread().getName(), record.partition(), record.offset(), record.key(), record.value());
//                }
//                retry--;
//            }
//            if(retry==0|| counter == 0){
//                counter = 0;
//                break;
//            }
//            else{
//                System.out.println(String.format("%s Retry LEFT...", retry);
//            }
//        }
//    }


}
