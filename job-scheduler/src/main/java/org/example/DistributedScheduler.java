package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class JobConfig {
    public String name;
    public int intervalSeconds;
    public String cronExpression;
    public String topic;
    public String payload;
}

public class DistributedScheduler {
    private static final Logger log = LoggerFactory.getLogger(DistributedScheduler.class.getSimpleName());
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    static void main(String[] args) {
        log.info("=====DistributedScheduler=====");

        // create producer properties
        Properties props = new Properties();

        // set producer properties
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        props.setProperty("bootstrap.servers", bootstrapServers != null ? bootstrapServers : "127.0.0.1:9092");

        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // load configuration
        ObjectMapper mapper = new ObjectMapper();

        List<JobConfig> jobs = null;
        try {
            String jobFile = System.getenv("JOB_FILE");
            jobs = mapper.readValue(new File( jobFile != null ? jobFile : "src/main/resources/jobs.json"), new TypeReference<List<JobConfig>>(){});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // schedule jobs
        for (JobConfig job : jobs) {

            if (job.intervalSeconds <= 0) {
                log.warn("Skipping job {} because intervalSeconds is missing or 0", job.name);
                continue;
            }

            log.info("Scheduling job: {} to run every {} seconds", job.name, job.intervalSeconds);

            // Using Java's built-in scheduler to trigger the Kafka push
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    log.info("Triggering Job: {}", job.name);

                    String messageKey = UUID.randomUUID().toString();

                    // The "Event" that triggers the work
                    ProducerRecord<String, String> record = new ProducerRecord<>(job.topic, messageKey, job.payload);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Failed to queue job {}", job.name, exception);
                        } else {
                            log.info("Job {} queued to partition {}", job.name, metadata.partition());
                        }
                    });
                } catch (Exception e) {
                    log.error("Error in scheduler loop", e);
                }
            }, 0, job.intervalSeconds, TimeUnit.SECONDS);
        }
        // keep the main thread alive
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down scheduler");
            producer.close();
        }));

    }
}
