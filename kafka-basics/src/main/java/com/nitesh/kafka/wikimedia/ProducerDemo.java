package com.nitesh.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("This is a Kafka Producer!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "certain-tuna-13374-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y2VydGFpbi10dW5hLTEzMzc0JLb-c9LWZBk9tcyLrBfsUKPg0_9n_BSzw73IGAk\" password=\"NmQyMGZlM2ItYWE5Yi00MWI5LWE1NDgtMjZlZDNmNzU2NjYx\";");


        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("my_demo_topic", "hey there");

        // send data
        producer.send(record);

        // tell the producer to send all data and block until done - synchronous
        // in actual prod cose, flush won't be needed all the time
        // as send() itself will asynchronously send all data
        producer.flush();

        // close - will also called close
        producer.close();
    }
}
