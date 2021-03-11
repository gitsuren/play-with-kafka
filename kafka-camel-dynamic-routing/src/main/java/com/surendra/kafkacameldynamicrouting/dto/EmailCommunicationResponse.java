package com.surendra.kafkacameldynamicrouting.dto;

import javax.xml.bind.annotation.*;

@XmlRootElement(name = "EmailCommunicationResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {"acknowledgementResponse"})
public class EmailCommunicationResponse {

    @XmlElement(name="acknowledgementResponse", required = true)
    protected Acknowledgement acknowledgementResponse;

    public Acknowledgement getAcknowledgementResponse() {
        return acknowledgementResponse;
    }

    public void setAcknowledgementResponse(Acknowledgement acknowledgementResponse) {
        this.acknowledgementResponse = acknowledgementResponse;
    }
}
