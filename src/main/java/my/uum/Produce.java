package my.uum;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * The ProduceMessage class is responsible for producing messages to a Kafka topic.
 * This class connects to a Kafka broker and sends messages fetched dynamically from an external API to the topic "student_info".
 */
public class Produce {

    /**
     * Fetches data from an external API.
     *
     * @param apiUrl The URL of the API to fetch data from.
     * @return The response from the API as a string.
     * @throws Exception If an error occurs during the API call.
     */
    private static String fetchDataFromApi(String apiUrl) throws Exception {
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            response.append(line);
        }

        reader.close();
        return response.toString();
    }

    /**
     * The main method sets up the Kafka producer properties, creates a producer, and sends messages to the Kafka topic.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "jsonplaceholder";
        String apiUrl = "https://jsonplaceholder.typicode.com/users"; // Free API endpoint

        try {
            // Fetch data from API and produce messages
            String apiResponse = fetchDataFromApi(apiUrl);

            // Parse the API response as JSON
            JSONArray users = new JSONArray(apiResponse);

            for (int i = 0; i < users.length(); i++) {
                JSONObject user = users.getJSONObject(i);
                int id = user.getInt("id");
                String name = user.getString("name");
                String username = user.getString("username");
                String email = user.getString("email");
                JSONObject address = user.getJSONObject("address");
                String street = address.getString("street");
                String suite = address.getString("suite");
                String city = address.getString("city");
                String zipcode = address.getString("zipcode");
                JSONObject geo = address.getJSONObject("geo");
                String lat = geo.getString("lat");
                String lng = geo.getString("lng");
                String phone = user.getString("phone");
                String website = user.getString("website");
                JSONObject company = user.getJSONObject("company");
                String companyName = company.getString("name");
                String catchPhrase = company.getString("catchPhrase");
                String bs = company.getString("bs");

                // Create a JSON string with all user information
                String messageKey = "User ID: " + id;
                String messageValue = new JSONObject()
                        .put("id", id)
                        .put("name", name)
                        .put("username", username)
                        .put("email", email)
                        .put("address", new JSONObject()
                                .put("street", street)
                                .put("suite", suite)
                                .put("city", city)
                                .put("zipcode", zipcode)
                                .put("geo", new JSONObject()
                                        .put("lat", lat)
                                        .put("lng", lng)))
                        .put("phone", phone)
                        .put("website", website)
                        .put("company", new JSONObject()
                                .put("name", companyName)
                                .put("catchPhrase", catchPhrase)
                                .put("bs", bs))
                        .toString();

                producer.send(new ProducerRecord<>(topic, messageKey, messageValue));
                System.out.println("Sent message - Key: " + messageKey + ", Value: " + messageValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}