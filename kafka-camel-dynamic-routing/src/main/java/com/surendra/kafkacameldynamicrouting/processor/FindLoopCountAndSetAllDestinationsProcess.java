package com.surendra.kafkacameldynamicrouting.processor;

import com.surendra.kafkacameldynamicrouting.common.Constant;
import com.surendra.kafkacameldynamicrouting.properties.RoutingProperties;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.xml.XPathBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FindLoopCountAndSetAllDestinationsProcess implements Processor{
    @Autowired
    private RoutingProperties routingProperties;

    @Override
    public void process(Exchange exchange) throws Exception {
        int loop = 0;

        KafkaManualCommit kafkaManualCommit = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        String topicName="";
        boolean isTransformationRequired = true;
        //Depending on the predicate the topic will be set
        if(XPathBuilder.xpath("EmailCommunicationResponse/acknowledgementResponse/userId != 'testUser'").matches(exchange))
            topicName = routingProperties.getConsumerTopic1();
        if(XPathBuilder.xpath("EmailCommunicationResponse/acknowledgementResponse/userId = 'testUser'").matches(exchange))
            topicName = routingProperties.getConsumerTopic2();
        if(XPathBuilder.xpath("/Notification/Category = 'CLOSING'").matches(exchange)) {
            topicName = routingProperties.getConsumerTopic3();
            isTransformationRequired = false;
        }

        //setting the valid attribute in the payload (transforming the payload)

        if(!topicName.isEmpty()){
            loop = 0;
        }
        else {
            loop = 1;
        }
        exchange.getIn().setHeader(Constant.TOPIC_NAME, topicName);
        exchange.getIn().setHeader(Constant.LOOP, loop);
        exchange.getIn().setHeader("isTransformationRequired", isTransformationRequired);
        kafkaManualCommit.commitSync();


    }
}
