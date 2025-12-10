package com.eda.demos.kafka;

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


    static void main(String[] args) {
        log.info("Hi I am the kafka producer with Callback");


        // create producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("batch.size", "400");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {

                // create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("java-demo", "Hello World " + i);


                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes everytime a record is successfully sent or an error is thrown
                        if (e == null) {
                            log.info("Received new metadata" + "\n" +
                                    "topic: " + metadata.topic() + "\n" +
                                    "partition: " + metadata.partition() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n" +
                                    "offset: " + metadata.offset()
                            );
                        } else {
                            log.error("Error while producing ", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }


        // flush and close producer

        // tell the producer to send all data and block until done.
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
