import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // Set up producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.29.158:9092"); // Kafka broker(s) address
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create a Kafka producer instance
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Produce a message to a Kafka topic
        String topic = "my-topic"; // Change this to the desired topic name
        String key = "key1";
        String value = "Hello, Kafka!";

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        // Send the record
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Message sent successfully. Topic: " + metadata.topic() +
                        ", Partition: " + metadata.partition() +
                        ", Offset: " + metadata.offset());
            } else {
                System.err.println("Error sending message: " + exception.getMessage());
            }
        });

        // Close the producer when done
        producer.close();
    }
}
