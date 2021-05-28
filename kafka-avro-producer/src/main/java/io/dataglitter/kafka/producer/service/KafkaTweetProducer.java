package io.dataglitter.kafka.producer.service;

import io.dataglitter.kafka.commons.generics.SchemaAdaptor;
import io.dataglitter.kafka.producer.schema.TweetFactory;
import org.apache.avro.generic.GenericRecord;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.stereotype.Service;
import twitter4j.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by reddys on 10/03/2018.
 */
@Service
public class KafkaTweetProducer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTweetProducer.class);

    @Value("${twitter.hashtags}")
    private String hashtags[];

    @Value("${kafka.topic}")
    private String topic;

    @Value("${schema.version}")
    private String tweetSchemaVersion;

    @Autowired
    private KafkaTemplate<String, GenericRecord> template;

    @Autowired
    private TweetFactory tweetFactory;

//    @Override
//    public void run(String... args) throws Exception {
//        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
//        SchemaAdaptor tweetAdaptor = tweetFactory.getSchemaAdaptor(tweetSchemaVersion);
//        twitterStream.addListener(new StatusAdapter() {
//            @Override
//            public void onStatus(Status status) {
//            template.send(topic, (GenericRecord) tweetAdaptor.populateRecord(status));
//            logger.info(Long.toString(status.getId()) + " " + tweetSchemaVersion);
//            }
//        });
//        twitterStream.filter(getFilters());
//    }

    @Override
    public void run(String... args) throws Exception {
        jsonParser();
    }



    public FilterQuery getFilters(){
        FilterQuery filters = new FilterQuery();
        filters.track(hashtags);
        return filters;
    }

    public void jsonParser()
    {
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader("C:\\Users\\BAJRACHARYA\\work\\projects\\KAFKA\\AVRO\\kafka-avro-producer\\src\\main\\resources\\truck_engine_sensors.json"))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);

            org.json.simple.JSONArray truckSensors = (org.json.simple.JSONArray) obj;

            truckSensors.forEach(
                    ts -> template.send("TRUCK_ENGINE_SENSORS", (GenericRecord) ts)
            );


            //Iterate over employee array
//            employeeList.forEach( emp -> parseEmployeeObject( (JSONObject) emp ) );

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
