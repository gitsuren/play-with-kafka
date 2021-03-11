package com.surendra.kafkacameldynamicrouting.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "camel.springboot")
@Component
public class RoutingProperties {

    private String producerTopic;
    private String consumerTopic1;
    private String consumerTopic2;
    private String consumerTopic3;
    private String auditTopic;
    private String bootstrapServer;
    private String kafkaUrl;
    private String failedTopic;
    private String repoFilePath;

    public String getProducerTopic() {
        return producerTopic;
    }

    public void setProducerTopic(String producerTopic) {
        this.producerTopic = producerTopic;
    }

    public String getConsumerTopic1() {
        return consumerTopic1;
    }

    public void setConsumerTopic1(String consumerTopic1) {
        this.consumerTopic1 = consumerTopic1;
    }

    public String getConsumerTopic2() {
        return consumerTopic2;
    }

    public void setConsumerTopic2(String consumerTopic2) {
        this.consumerTopic2 = consumerTopic2;
    }

    public String getConsumerTopic3() {
        return consumerTopic3;
    }

    public void setConsumerTopic3(String consumerTopic3) {
        this.consumerTopic3 = consumerTopic3;
    }

    public String getAuditTopic() {
        return auditTopic;
    }

    public void setAuditTopic(String auditTopic) {
        this.auditTopic = auditTopic;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getKafkaUrl() {
        return kafkaUrl;
    }

    public void setKafkaUrl(String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }

    public String getFailedTopic() {
        return failedTopic;
    }

    public void setFailedTopic(String failedTopic) {
        this.failedTopic = failedTopic;
    }

    public String getRepoFilePath() {
        return repoFilePath;
    }

    public void setRepoFilePath(String repoFilePath) {
        this.repoFilePath = repoFilePath;
    }
}
