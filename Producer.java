final Properties producerProps = new Properties();
producerProps.setProperty("bootstrap.servers", "localhost:9092");
producerProps.setProperty("key.serializer",
                          StringSerializer.class.getName());
producerProps.setProperty("value.serializer", 
                          StringSerializer.class.getName());
producerProps.setProperty("max.in.flight.requests.per.connection", 
                          String.valueOf(1));try (Producer<String, String> producer = 
     new KafkaProducer<>(producerProps)) {
  while (true) {
    final String value = new Date().toString();
    System.out.format("Publishing record with value %s%n", value);
    producer.send(new ProducerRecord<>("test", "myKey", value));
    Thread.sleep(1000);
  }
