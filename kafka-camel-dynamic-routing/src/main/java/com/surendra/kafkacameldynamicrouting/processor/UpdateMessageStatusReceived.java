package com.surendra.kafkacameldynamicrouting.processor;

import com.surendra.kafkacameldynamicrouting.common.Constant;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public class UpdateMessageStatusReceived implements Processor {


    @Override
    public void process(Exchange exchange) throws Exception {
        try{
            exchange.getIn().setHeader(Constant.MESSAGE_STATUS, Constant.RECEIVED);
        }
        catch(Exception e){
            throw new Exception(e.getMessage());
        }
    }
}
