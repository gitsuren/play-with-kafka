package com.surendra.kafkacameldynamicrouting.routes;

import com.surendra.kafkacameldynamicrouting.processor.FindLoopCountAndSetAllDestinationsProcess;
import com.surendra.kafkacameldynamicrouting.processor.UpdateMessageStatusDiscarded;
import com.surendra.kafkacameldynamicrouting.processor.UpdateMessageStatusReceived;
import com.surendra.kafkacameldynamicrouting.properties.RoutingProperties;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.apache.camel.impl.FileStateRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.rmi.UnmarshalException;

@Component
public class PayloadRoute extends RouteBuilder {

    @Autowired
    private RoutingProperties routingProperties;

    @Autowired
    private FindLoopCountAndSetAllDestinationsProcess genericProcessor;

    @Autowired
    private UpdateMessageStatusDiscarded updateMessageStatusDiscarded;

    @Autowired
    private UpdateMessageStatusReceived updateMessageStatusReceived;


    @Override
    public void configure() throws Exception {
        //Exception Handling invalid payload. Placing the failed payload on to a topic for failed payloads

        onException(UnmarshalException.class, Exception.class)
                .handled(true)
                .process(exchange -> {
                    Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    System.out.println("The exception is :" + cause.getLocalizedMessage() + "\n" + cause.getMessage()  + "\n"  + cause.getCause());
                    KafkaManualCommit kafkaManualCommit = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
                    kafkaManualCommit.commitSync();
                })
                .maximumRedeliveries(2)
                .to(String.format(routingProperties.getKafkaUrl(),
                        routingProperties.getFailedTopic(),
                        routingProperties.getBootstrapServer()));

        from("file:src/data")
                .to(String.format(routingProperties.getKafkaUrl(),
                        routingProperties.getProducerTopic(),
                        routingProperties.getBootstrapServer()));

        //dynamic routing of the message depending on the content and manual commit
        from(String.format(routingProperties.getKafkaUrl(),
                routingProperties.getProducerTopic(),
                routingProperties.getBootstrapServer())
                + "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true&offsetRepository=#fileStore")
            .log("message received on the producer queue ${body}")
            .log(" on the topic ${header[kafka.TOPIC]}")
            .process(updateMessageStatusReceived)
            .process(genericProcessor)
            .to("direct:publishToAudit")
            .choice()
            .when(simple("${in.header.loop} > 0"))
            .to("direct:transform")
            .otherwise()
            .process(updateMessageStatusDiscarded)
            .to("direct:publishToAudit");

        //Publish to AUDIT QUeue
        from("direct:publishToAudit")
                    .to(String.format(routingProperties.getKafkaUrl(), routingProperties.getAuditTopic(), routingProperties.getBootstrapServer()));

        // Transform ROUTE
        from("direct:transform")
                .choice()
                .when(simple("${in.header.isTransformRequired}"))
                .to("xslt:file:/src/xslt/Surendra.xsl")
                .toD(String.format(routingProperties.getKafkaUrl(), "${header.topicName}", routingProperties.getBootstrapServer()))
                .otherwise()
                .toD(String.format(routingProperties.getKafkaUrl(), "${header.topicName}", routingProperties.getBootstrapServer()));

        //To Consume the message from the consumer

        from(String.format(routingProperties.getKafkaUrl(), routingProperties.getConsumerTopic1(), routingProperties.getBootstrapServer()) + "&autoOffsetReset=latest")
                .log("Message Status  ${header.messageStatus}")
                .log("Message received on TOPIC 1 : ${body}");

        from(String.format(routingProperties.getKafkaUrl(), routingProperties.getConsumerTopic2(), routingProperties.getBootstrapServer()) + "&autoOffsetReset=latest")
                .log("Message Status  ${header.messageStatus}")
                .log("Message received on TOPIC2 : ${body}");

        from(String.format(routingProperties.getKafkaUrl(), routingProperties.getConsumerTopic3(), routingProperties.getBootstrapServer()) + "&autoOffsetReset=latest")
                .log("Message Status  ${header.messageStatus}")
                .log("Message received on TOPIC3 : ${body}");

        from(String.format(routingProperties.getKafkaUrl(), routingProperties.getAuditTopic(), routingProperties.getBootstrapServer()) + "&autoOffsetReset=latest")
                .log("Message Status  ${header.messageStatus}")
                .log("Message received on AUDIT TOPIC : ${body}");

    }

    @Bean(name="fileStore", initMethod="start", destroyMethod="stop")
    private FileStateRepository fileStore(){
        File f = new File(routingProperties.getRepoFilePath());
        FileStateRepository fileStateRepository = FileStateRepository.fileStateRepository(f);
        fileStateRepository.setMaxFileStoreSize(1048760);// 10 MB max
        return fileStateRepository;
    }
}
