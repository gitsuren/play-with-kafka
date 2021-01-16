package com.surendra.kakfaoffsetmgmt.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Service
public class SensorConsumer {

    String topicName = "SensorTopic";

    Properties properties = null;
    private int rCount;

    @PostConstruct
    public void init() {
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "false");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    //    @Override
    public void run() {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor.consumer.false.auto.commit.group.1");

        TopicPartition topicPartition0 = new TopicPartition(topicName, 0);
        TopicPartition topicPartition1 = new TopicPartition(topicName, 1);
        TopicPartition topicPartition2 = new TopicPartition(topicName, 2);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.assign(Arrays.asList(topicPartition0, topicPartition1, topicPartition2));

        System.out.println("Current position P0 = " + consumer.position(topicPartition0)
                + " position P1 = " + consumer.position(topicPartition1) +
                "position P0 = " + consumer.position(topicPartition0));

        consumer.seek(topicPartition0, getOffsetFromDB(topicPartition0));
        consumer.seek(topicPartition1, getOffsetFromDB(topicPartition1));
        consumer.seek(topicPartition2, getOffsetFromDB(topicPartition2));

        System.out.println("start Fetching NOW");

        try {
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                rCount = records.count();
                System.out.println("Records Polled: " + rCount);
                for (ConsumerRecord record : records) {
                    saveAndCommit(consumer, record);
                }

            }
            while (rCount > 0);
        } catch (Exception e) {
            System.out.println("Exception in run");
        } finally {
            consumer.close();
        }

    }

    private void saveAndCommit(KafkaConsumer<String, String> consumer, ConsumerRecord record) {
        System.out.println(
                "Topic = " + record.topic() +
                        "Partition = " + record.partition() +
                        "Offset = " + record.offset() +
                        "Key = " + record.key() +
                        "Value = " + record.value()
        );

        try {
            String insert = "insert into tss_data(skey, svalue, topic_name, partitionn, offsett) values (?,?,?,?,?)";
            jdbcTemplate.update(insert, record.key(), record.value(), record.topic(), record.partition(), record.offset());

            String update = "update tss_offsets set offsett = ? where topic_name=? and partitionn=?";
            jdbcTemplate.update(update, record.offset() + 1, record.topic(), record.partition());
        } catch (Exception e) {

        }
    }

    private long getOffsetFromDB(TopicPartition topicPartition) {
        long offset = 0;

        try {

            String sql = String.format("select offsett from tss_offsets where topic_name=%s and partitionn=%s", topicPartition.topic(), topicPartition.partition());
            offset = jdbcTemplate.queryForList(sql)
                    .stream()
                    .map(m -> (Integer) m.get("offsett"))
                    .findAny()
                    .get();
            ;

        } catch (Exception e) {
            System.out.println("exception ");
        }

        return offset;
    }

    //https://blog.sysco.no/integration/kafka-rewind-consumers-offset/

    //REPLAY/RETRY

    public void replayMessage(String topicName, int partition, int offset) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor.consumer.false.auto.commit.group.2");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");//to make sure that I only get exactly one desired message on that offset

        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.seek(topicPartition, offset);
        System.out.println("Current Position tp = " + consumer.position(topicPartition));

        System.out.println("Start Fetching NOW");
        int counter = 1;// only get 1 message. double guards for max.poll.record
        int retry = 5; // retry 5 times then give up if cannot receive message

        while (counter > 0) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (retry > 0 || !consumerRecords.isEmpty()) {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    counter--;
                    System.out.printf("==========thread name: %s, partition: %s , offset:%s, key: %s, value: %s", Thread.currentThread().getName(), record.partition(), record.offset(), record.key(), record.value());
                }
                retry--;
            }
            if(retry==0|| counter == 0){
                counter = 0;
                break;
            }
            else{
                System.out.printf("%s Retry LEFT...", retry);
            }
        }
    }


}
