package com.surendra.kakfaoffsetmgmt.controller;

import com.surendra.kakfaoffsetmgmt.service.consumer.SensorConsumer;
import com.surendra.kakfaoffsetmgmt.service.consumer.SensorConsumerWithAutoCommitTrue;
import com.surendra.kakfaoffsetmgmt.service.producer.SensorProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class SensorController {

    private static Logger logger = LoggerFactory.getLogger(SensorController.class);

    @Autowired
    private SensorConsumer sensorConsumer;

    @Autowired
    private SensorConsumerWithAutoCommitTrue sensorConsumerWIthAutoCommitTrue;

    @Autowired
    private SensorProducer sensorProducer;

    @GetMapping("/sensor/{message}")
    public String sendMessageToSensor(@PathVariable String message){
        logger.info("---Message Received---");
        sensorProducer.produce("Hello " + message);
        return "Message sent to Kafka sensor successfully";
    }

    @GetMapping("/runSensorWithoutAutoCommit")
    public String runSensorWithoutAutoCommit(){
        logger.info("runSensorWithoutAutoCommit started-----");
        sensorConsumer.run();
        return "Sensor work completed --- runSensorWithoutAutoCommit";
    }

    @GetMapping("/runSensorWithAutoCommit")
    public String runSensorWithAutoCommit(){
        logger.info("runSensorWithAutoCommit started-----");
        sensorConsumerWIthAutoCommitTrue.simpleConsumingMessage();
        return "Sensor work completed --- runSensorWithAutoCommit";
    }

    @GetMapping("/runSensorWithoutAutoCommitReplay/{topicName}/{partition}/{offset}")
    public String runSensorWithoutAutoCommitReplay(@PathVariable  String topicName, @PathVariable int partition, @PathVariable int offset){
        logger.info("runSensorWithoutAutoCommitReplay started-----");
        sensorConsumer.replayMessage(topicName, partition, offset);
        return "Sensor work completed --- runSensorWithoutAutoCommitReplay";
    }

    @GetMapping("/runSensorWithoutAutoCommitReplayFromTheBeginning/{topicName}/{partition}")
    public String runSensorWithoutAutoCommitReplayFromTheBeginning(@PathVariable  String topicName, @PathVariable int partition){
        logger.info("runSensorWithoutAutoCommitReplayFromTheBeginning started-----");
        sensorConsumerWIthAutoCommitTrue.replayMessageToBeginning(topicName, partition);
        return "Sensor work completed --- runSensorWithoutAutoCommitReplayFromTheBeginning";
    }

    @GetMapping("/runSensorWithoutAutoCommitReplayFromSpecificOffset/{topicName}/{partition}/{offset}")
    public String runSensorWithoutAutoCommitReplayFromSpecificOffset(@PathVariable  String topicName, @PathVariable int partition, @PathVariable int offset){
        logger.info("runSensorWithoutAutoCommitReplayFromSpecificOffset started-----");
        sensorConsumerWIthAutoCommitTrue.replayMessageFromSpecificOffset(topicName, partition, offset);
        return "Sensor work completed --- runSensorWithoutAutoCommitReplayFromSpecificOffset";
    }

    @GetMapping("/runSensorWithoutAutoCommitReplayFromTimeStamp/{topicName}/{partition}/{timeInMinutes}")
    public String runSensorWithoutAutoCommitReplayFromTimeStamp(@PathVariable  String topicName, @PathVariable int partition, @PathVariable int timeInMinutes){
        logger.info("runSensorWithoutAutoCommitReplayFromTimeStamp started-----");
        sensorConsumerWIthAutoCommitTrue.replayMessageByTimeStamp(topicName, partition, timeInMinutes);
        return "Sensor work completed --- runSensorWithoutAutoCommitReplayFromTimeStamp";
    }


}
