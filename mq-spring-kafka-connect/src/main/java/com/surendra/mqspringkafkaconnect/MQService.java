package com.surendra.mqspringkafkaconnect;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

@Service
@EnableJms
public class MQService {

    @Autowired
    private JmsTemplate jmsTemplate;
}
