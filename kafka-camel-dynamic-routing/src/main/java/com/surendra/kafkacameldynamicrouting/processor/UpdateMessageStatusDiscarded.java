package com.surendra.kafkacameldynamicrouting.processor;

import com.surendra.kafkacameldynamicrouting.common.Constant;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public class UpdateMessageStatusDiscarded implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        try{
            exchange.getIn().setHeader(Constant.MESSAGE_STATUS, Constant.DISCARDED);
        }
        catch(Exception e){
            throw new Exception(e.getMessage());
        }
    }
}
