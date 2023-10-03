import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {
    public static void main(String[] args) {
        // Set up consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.29.158:9092"); // Kafka broker(s) address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group"); // Consumer group ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create a Kafka consumer instance
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to a Kafka topic
        String topic = "my-topic"; // Change this to the desired topic name
        consumer.subscribe(Collections.singletonList(topic));

        // Poll for messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Adjust the polling interval as needed

            records.forEach(record -> {
                System.out.println("Received message: " +
                        "Topic = " + record.topic() +
                        ", Partition = " + record.partition() +
                        ", Offset = " + record.offset() +
                        ", Key = " + record.key() +
                        ", Value = " + record.value());
            });
        }
    }
}
