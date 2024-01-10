package com.nitesh.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello World");

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

        // Sticky Partitioner: Default partitioner in Kafka which send a batch of
        // messages to the same partition(until batch.size which is 16KB by default)
        // before sending a new batch to a different partition.
        for(int i=0; i<10; i++) {
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("my_demo_topic", "hey there" + i);

            // send data
            producer.send(record, new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception ex) {
                    // this is called everytime a record is successfuly sent
                    // or an exception is thrown while sending it
                    if (ex == null) {
                        log.info("Received new metadata: \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n");
                    } else {
                        log.info("Exception occured: " + ex.toString());
                    }
                }
            });
        }



        // tell the producer to send all data and block until done - synchronous
        // in actual prod cose, flush won't be needed all the time
        // as send() itself will asynchronously send all data
        producer.flush();

        // close - will also called close
        producer.close();
    }
}
