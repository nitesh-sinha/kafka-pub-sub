package com.nitesh.kafka.wikimedia;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithGracefulShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithGracefulShutdown.class.getSimpleName());
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
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName()); // use this for using the incremental assignment strategy


        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        // Add a shutdown hook which will be executed in a new thread
        // when the consumer is stopped while it is running. We need a graceful
        // shutdown of consumer i.e. all the offsets committed successfully,
        // connection to kafka closed, groups to rebalance etc.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown, lets exit by calling consumer wakeup");
                // When we stop the consumer, this new thread starts running
                // and calls consumer.wakeup() which will cause the mainThread(which is
                // waiting on consumer.poll()) to throw a WakeupException. This will be
                // caught by the main thread in consumer.close() and then finally block runs to commit
                // the offsets. All this will happen in main thread while this new thread
                // is keeping the consumer process alive by waiting on the main thread
                // as part of mainThread.join().
                consumer.wakeup();


                // join main thread to allow execution of code
                // in main thread to fully complete
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // Retrieve data from topic
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " Value: " + record.value());
                    log.info("Parititon: " + record.partition() + " Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            // THis is expected when consumer is stopped while it is running
            log.info("Consumer will start to gracefully shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in consumer: ", e);
        } finally {
            consumer.close(); // this will commit the offsets and close the consumer
            log.info("Consumer is now gracefully shutdown.");
        }
    }
}


/*

Log of the shutdown process:

[main] INFO ConsumerDemoWithGracefulShutdown - Polling
[main] INFO ConsumerDemoWithGracefulShutdown - Polling
[Thread-0] INFO ConsumerDemoWithGracefulShutdown - Detected shutdown, lets exit by calling consumer wakeup
[main] INFO ConsumerDemoWithGracefulShutdown - Consumer will start to gracefully shut down
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-my-hava-app-1, groupId=my-hava-app] Revoke previously assigned partitions my_demo_topic-1, my_demo_topic-0, my_demo_topic-2
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-my-hava-app-1, groupId=my-hava-app] Member consumer-my-hava-app-1-a4836ee1-d01a-4d42-80d5-c583072fdf75 sending LeaveGroup request to coordinator certain-tuna-13374-eu2-1-kafka.upstash.io:9093 (id: 2147483647 rack: null) due to the consumer is being closed
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-my-hava-app-1, groupId=my-hava-app] Resetting generation due to: consumer pro-actively leaving the group
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-my-hava-app-1, groupId=my-hava-app] Request joining group due to: consumer pro-actively leaving the group
[main] INFO org.apache.kafka.common.metrics.Metrics - Metrics scheduler closed
[main] INFO org.apache.kafka.common.metrics.Metrics - Closing reporter org.apache.kafka.common.metrics.JmxReporter
[main] INFO org.apache.kafka.common.metrics.Metrics - Metrics reporters closed
[main] INFO org.apache.kafka.common.utils.AppInfoParser - App info kafka.consumer for consumer-my-hava-app-1 unregistered
[main] INFO ConsumerDemoWithGracefulShutdown - Consumer is now gracefully shutdown.

Process finished with exit code 130 (interrupted by signal 2: SIGINT)


 */