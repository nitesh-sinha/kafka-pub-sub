package com.nitesh.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
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
        String topic = "my_demo_topic";

        for(int j=0; j<3; j++) {
            for(int i=0; i<10; i++) {
                // Messages with same key should go to same partition
                String key = "id_" + i;
                String value = "hello " + i;

                // create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(record, new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception ex) {
                        // this is called everytime a record is successfuly sent
                        // or an exception is thrown while sending it
                        if (ex == null) {
                            log.info("Received new metadata: \n" +
                                    "Key: " + key + "\n" +
                                    "Partition: " + metadata.partition() + "\n");
                        } else {
                            log.info("Exception occured: " + ex.toString());
                        }
                    }
                });
                // Adding the sleep to batch the messages and send them
                // to different partitions - else stickypartitioner
                // will send all to same partition in a single batch
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }




        // tell the producer to send all data and block until done - synchronous
        // in actual prod cose, flush won't be needed all the time
        // as send() itself will asynchronously send all data
        producer.flush();

        // close - will also called close
        producer.close();
    }
}
