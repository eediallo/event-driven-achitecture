package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class JobConsumer {
    private static final Logger log = LoggerFactory.getLogger(JobConsumer.class.getSimpleName());

    static void main(String[] args) {
        log.info("===CONSUMER has started=====");

        // set consumer properties
        Properties props = new Properties();

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers != null ? bootstrapServers : "127.0.0.1:9092");

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "job-execution-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // if there is no offset, start from the lates
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // subscribe to the topic
        String topic = System.getenv("TOPIC_NAME");
        consumer.subscribe(Collections.singletonList(topic != null ? topic : "db-ops"));


        try {
            while (true) {
                // Poll for new data
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("<<< RECEIVED Job | Partition: {} | Key: {} | Payload: {}",
                            record.partition(), record.key(), record.value());

                    // --- EXECUTION LOGIC HERE ---
                    // In the future, we will Runtime.exec() the record.value()
                    // For now, we just acknowledge receipt
                    executeJob(record.value());
                }
            }
        } catch (Exception e) {
            log.error("Error in consumer", e);
        } finally {
            consumer.close();
        }
    }

    private static void executeJob(String command) {
        // Simulating work
        log.info("Executing command: [{}]", command);
    }

}

