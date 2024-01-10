package com.nitesh.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaEventHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class.getName());

    WikimediaEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.kafkaProducer = producer;
        this.topic = topic;
    }


    @Override
    public void onOpen() {
        // do nothing
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        String data = messageEvent.getData();
        log.info("Sending event to Kafka: " + data);
        // async send to kafka
        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(String comment) throws Exception {
        // do nothing
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error while reading the stream: " + t);
    }
}
