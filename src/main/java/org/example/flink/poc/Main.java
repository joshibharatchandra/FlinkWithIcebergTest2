package org.example.flink.poc;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        // Step 2 -> Load credentials from the downloaded JSON file
        GoogleCredentials googleCredentials = GoogleCredentials.fromStream(Main.class.getClassLoader().getResourceAsStream("wired-record-443707-u8-618011b336b0.json"));
        DeserializationSchema<JsonNode> deserializer = new DeserializationSchema<JsonNode>() {
            @Override
            public JsonNode deserialize(byte[] message) throws IOException {
                // Deserialize the message to a JsonNode (or just a String if your message is plain text)
                return new ObjectMapper().readTree(message);
            }

            @Override
            public boolean isEndOfStream(JsonNode nextElement) {
                return false;
            }

            @Override
            public TypeInformation<JsonNode> getProducedType() {
                return TypeInformation.of(JsonNode.class);
            }
        };

        // Step 4 -> Set up the Pub/Sub source to read messages from the topic
        PubSubSource<JsonNode> pubSubSource = PubSubSource.newBuilder()
                .withDeserializationSchema(deserializer)
                .withProjectName("wired-record-443707-u8")  // Replace with your Google Cloud project ID
                .withSubscriptionName("flink-test-topic-sub")  // Replace with your subscription name
                .withCredentials(googleCredentials)  // Use the loaded credentials
                .build();

        // Step 5 -> Add the Pub/Sub source to the Flink job
        DataStream<JsonNode> pubSubMessages = env.addSource(pubSubSource);
        pubSubMessages.map(jsonNode -> jsonNode.toString());

        // Step 6 -> Map each message to a simple String or print the message directly
        pubSubMessages
                .map(jsonNode -> {
                    System.out.println("Received Message: " + jsonNode.toString());
                    return jsonNode.toString(); // Convert the JsonNode to a String for further processing
                }); // This ensures the pipeline is executed and outputs to the console

        // Step 7 -> Execute the Flink job (this will start consuming messages from Pub/Sub)
        env.execute("Flink Pub/Sub Message Reader");
    }
}