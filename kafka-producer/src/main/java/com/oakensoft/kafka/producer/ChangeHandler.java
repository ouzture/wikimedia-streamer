package com.oakensoft.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(ChangeHandler.class.getName());

    private final KafkaProducer<String,String> producer;
    private final String topic;

    public ChangeHandler(KafkaProducer<String,String> producer, String topic){
       this.producer = producer;
       this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("ChangeHandler opened");
    }

    @Override
    public void onClosed()  {
        log.info("Closing kafka producer");
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent)  {
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s)  {
        log.info(String.format("Got comment:%s",s));
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error on change handler",throwable);
    }
}
