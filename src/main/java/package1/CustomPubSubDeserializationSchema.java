package package1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;

import java.nio.charset.StandardCharsets;

// Custom Pub/Sub deserialization schema
public class CustomPubSubDeserializationSchema implements PubSubDeserializationSchema<JsonNode> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(JsonNode nextElement) {
        // Always return false since there is no explicit end of stream
        return false;
    }

    @Override
    public JsonNode deserialize(PubsubMessage pubsubMessage) throws Exception {
        byte[] data = pubsubMessage.getData().toByteArray();

        try {
            // Convert the byte array to a JsonNode
            return objectMapper.readTree(new String(data, StandardCharsets.UTF_8));
        } catch (Exception e) {
            // Log error and skip message if deserialization fails
            System.err.println("Failed to deserialize Pub/Sub message: " + e.getMessage());
        }
        return null;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        // Return the produced type (JsonNode in this case)
        return TypeInformation.of(JsonNode.class);
    }
}


