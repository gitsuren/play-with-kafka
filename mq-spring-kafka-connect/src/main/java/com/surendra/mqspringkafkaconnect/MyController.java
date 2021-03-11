package com.surendra.mqspringkafkaconnect;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MyController {

//    @Autowired
//    MQService mqService;

    @Autowired
    private JmsTemplate jmsTemplate;

    @GetMapping("send/{message}")
    String send(@PathVariable String message){
        try{
            jmsTemplate.convertAndSend("DEV.QUEUE.1", message);
            return "OK";
        }catch(JmsException ex){
            ex.printStackTrace();
            return "FAIL";
        }
    }

    @GetMapping("recv")
    String recv(){
        try{
            return jmsTemplate.receiveAndConvert("DEV.QUEUE.1").toString();
        }catch(JmsException ex){
            ex.printStackTrace();
            return "FAIL";
        }
    }
}
