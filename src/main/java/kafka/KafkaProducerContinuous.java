package kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerContinuous {
    public static void main(String[] args) throws InterruptedException {
        // Kafka broker properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create a Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send messages to a Kafka topic continuously
        String topic = "my_topic";
        String key = "key1";
        int messageCount = 0;

        while (true) {
            String value = "Message " + messageCount;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
            System.out.println("Sent: " + value);
            messageCount++;

            Thread.sleep(1000); // Add a 1-second delay between messages
        }
    }
}
