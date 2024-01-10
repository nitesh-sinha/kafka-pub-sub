package com.nitesh.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("This is a Kafka Consumer!");
        String groupId = "my-hava-app";
        String topic = "my_demo_topic";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "certain-tuna-13374-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y2VydGFpbi10dW5hLTEzMzc0JLb-c9LWZBk9tcyLrBfsUKPg0_9n_BSzw73IGAk\" password=\"NmQyMGZlM2ItYWE5Yi00MWI5LWE1NDgtMjZlZDNmNzU2NjYx\";");


        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // other possible value: none, latest


        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // Retrieve data from topic
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + " Value: " + record.value());
                log.info("Parititon: " + record.partition() + " Offset: " + record.offset());
            }
        }
    }
}
