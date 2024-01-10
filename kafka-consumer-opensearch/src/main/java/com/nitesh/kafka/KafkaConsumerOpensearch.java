package com.nitesh.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerOpensearch {
    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200"; // 9200 is the opensearch DB port

        RestHighLevelClient restHighLevelClient;
        // Build a URI from url
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "opensearch-demo-app";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the consumer
        return new KafkaConsumer<>(properties);
    }

    private static String generateId(ConsumerRecord<String, String> record) {
        // Strategy 1: Use the co-ordinates provided by Kafka
        // to generate this id
        //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

        // Strategy 2: Extract id from the Kafka message itself
        String id = JsonParser.parseString(record.value())
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();

        return id;
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(KafkaConsumerOpensearch.class.getName());
        // create an opensearch client
        RestHighLevelClient opensearchClient = createOpenSearchClient();

        // Create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        // Add a shutdown hook which will be executed in a new thread
        // when the consumer is stopped while it is running. We need a graceful
        // shutdown of consumer i.e. all the offsets committed successfully,
        // connection to kafka closed, groups to rebalance etc.
        final Thread mainThread = Thread.currentThread();

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
                kafkaConsumer.wakeup();


                // join main thread to allow execution of code
                // in main thread to fully complete
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Create index on Opensearch if it doesn't exist
        try(opensearchClient; kafkaConsumer) { // try-with-resource block will close the resource upon completion/error of this try block
            boolean indexExists = opensearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexReq = new CreateIndexRequest("wikimedia");
                opensearchClient.indices().create(createIndexReq, RequestOptions.DEFAULT);
                log.info("Wikimedia index got created successfully");
            } else {
                log.info("Index exists already!");
            }

            // subscribe to a topic
            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(5000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " messages");
                BulkRequest bulkRequest = new BulkRequest(); // more performant to write to opensearch in a batch instead of writing every record one by one
                for(ConsumerRecord<String, String> record : records) {
                    try{
                        // Make this consumer idempotent by generating an id here
                        // instead of letting OpenSearch generate an id by itself
                        // because OpenSearch will treat every new request to add a record
                        // to its index as a new request even though the data is duplicate
                        // when there are failures during processing and messages are re-read.

                        String id = generateId(record);
                        // send the record to openSearch
                        IndexRequest indexReq = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        //IndexResponse resp = opensearchClient.index(indexReq, RequestOptions.DEFAULT);
                        //log.info("Inserted 1 record into Opensearch with id: " + resp.getId());
                        bulkRequest.add(indexReq);
                    } catch (Exception e) {
                        // ignore for now
                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = opensearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records.");
                    try {
                        Thread.sleep(1000); // this sleep ensures more records are added to a batch for next read
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (WakeupException e) {
            // THis is expected when consumer is stopped while it is running
            log.info("Consumer will start to gracefully shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in consumer: ", e);
        } finally {
            kafkaConsumer.close(); // this will commit the offsets and close the consumer
            opensearchClient.close();
            log.info("Consumer is now gracefully shutdown.");
        }
    }
}
