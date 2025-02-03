package my.uum;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * The Consumer class is responsible for consuming messages from a Kafka topic.
 * This class connects to a Kafka broker, subscribes to the topic "student_info",
 * and continuously polls for new messages, printing them to the console.
 */
public class Consumer {

    /**
     * The main method sets up the Kafka consumer properties, creates a consumer, and subscribes to a Kafka topic.
     * It continuously polls for new messages from the topic and prints each received message to the console.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        // IP address of Producer
        props.put("bootstrap.servers", "ipPcProducer:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "student_info";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                // Poll for records with a timeout of 100 milliseconds
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println("Received message - " + key + ", " + value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
