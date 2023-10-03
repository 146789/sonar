package kafka;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerContinuous {
    public static void main(String[] args) {
        // Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Create a Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the Kafka topic
        String topic = "my_topic";
        consumer.subscribe(Collections.singletonList(topic));

        // Consume and print messages continuously
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                records.forEach(record -> {
                    System.out.println("Received message: " + record.value());
                });
            }
        } finally {
            // Close the consumer when done
            consumer.close();
        }
    }
}
