package my.uum;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * The ProduceMessage class is responsible for producing messages to a Kafka topic.
 * This class connects to a Kafka broker and sends a specified number of messages to the topic "student_info".
 * Each message consists of a key and a value, where the key is "Student ID: i" and the value is "Student Name: i".
 */
public class ProduceMessage {

    /**
     * The main method sets up the Kafka producer properties, creates a producer, and sends messages to the Kafka topic.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        // IPv4 address of Consumer
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        String topic = "student_info";

        try {
            // Produce messages
            for (int i = 0; i < 10; i++) {
                String messageKey = "Student ID: " + i;
                String messageValue = "Student Name: Student_" + i; // Modified message value
                producer.send(new ProducerRecord<>(topic, messageKey, messageValue));
                System.out.println(new ProducerRecord<>(topic, messageKey, messageValue));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
