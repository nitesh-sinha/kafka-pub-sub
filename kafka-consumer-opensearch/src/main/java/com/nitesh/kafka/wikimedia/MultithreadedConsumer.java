package com.nitesh.kafka.wikimedia;

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/*
    This Kafka consumer, in the main thread,
    reads a Kafka topic for new messages. However
    processing of each message is done in separate threads
    using a ThreadPoolExecutor. Sending email is synchronous
    but since it is used within a separate thread, the main
    thread will remain unblocked for reading from Kafka topic.

    We could send emails in threads asynchronously as well, if required.
 */
public class MultithreadedConsumer {
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "send-email-app";

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


    private static void sendEmailHelper(Session session, String toEmail, String subject, String body){
        try {
            MimeMessage msg = new MimeMessage(session);
            //set message headers
            msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
            msg.addHeader("format", "flowed");
            msg.addHeader("Content-Transfer-Encoding", "8bit");
            msg.setFrom(new InternetAddress("no_reply@example.com", "Info-NoReply"));
            msg.setReplyTo(InternetAddress.parse("no_reply@example.com", false));
            msg.setSubject(subject, "UTF-8");
            msg.setText(body, "UTF-8");
            msg.setSentDate(new Date());
            msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));

            System.out.println("Message is ready");
            Transport.send(msg);

            System.out.println("EMail Sent Successfully!!");
        }
        catch (Exception e) {
            System.out.println("Error while sending email out: " + e.toString());
            e.printStackTrace();
        }
    }

    private static void processKafkaMessage(String toEmail, String message) {
        final String fromEmail = "<sender-gmail-id>";
        final String password = "<sender-app-password-NOT-browser-login-password>";

        System.out.println("Sending an email to " + toEmail + " with message: " + message);
        Properties props = new Properties();
        props.put("mail.smtp.host", "smtp.gmail.com"); //SMTP Host
        props.put("mail.smtp.port", "587"); //TLS Port
        props.put("mail.smtp.auth", "true"); //enable authentication
        props.put("mail.smtp.starttls.enable", "true"); //enable STARTTLS

        //create Authenticator object to pass in Session.getInstance argument
        Authenticator auth = new Authenticator() {
            //override the getPasswordAuthentication method
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(fromEmail, password);
            }
        };

        // TODO: Exception from thread which calls this method
        // is not surfaced up in console. Possibly try logging it?
        Session session = Session.getInstance(props, auth);

        sendEmailHelper(session, toEmail,"Email while testing Kafka", message);
    }


    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(MultithreadedConsumer.class.getName());
        ExecutorService executorSvc = Executors.newFixedThreadPool(5);
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorSvc.shutdown();
            kafkaConsumer.close();
            log.info("Consumer is now gracefully shutdown.");
        }));

        // subscribe to a topic
        kafkaConsumer.subscribe(Collections.singleton("send-email"));

        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(5000));
            int recordCount = records.count();
            log.info("Received " + recordCount + " messages");
            for(ConsumerRecord<String, String> record : records) {
                try{
                    executorSvc.submit(() -> processKafkaMessage(record.key(), record.value()));
                } catch (Exception e) {
                    System.out.println("Exception occured: " + e.toString());
                }
            }
        }
    }
}
