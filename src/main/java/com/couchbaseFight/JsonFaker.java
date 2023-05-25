package com.couchbaseFight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.json.JSONObject;

import java.io.IOException;

public class JsonFaker {
    public  void main(String[] args) {
        // Your JsonNode schema
        JsonNode jsonSchema = getJsonSchema();

        // Generate fake JSON object based on the schema
        Faker faker = new Faker();
        JSONObject fakeJson = generateFakeJson(jsonSchema, faker);

        // Print the fake JSON object
        System.out.println(fakeJson.toString());
    }

    private static JsonNode getJsonSchema() {
        // Define your JsonNode schema here
        // Example schema:
        String schema = "{\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n" +
                "    \"name\": { \"type\": \"string\" },\n" +
                "    \"age\": { \"type\": \"integer\", \"minimum\": 18 }\n" +
                "  },\n" +
                "  \"required\": [ \"name\", \"age\" ]\n" +
                "}";

        // Parse the schema using ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readTree(schema);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static JSONObject generateFakeJson(JsonNode jsonSchema, Faker faker) {
        JSONObject jsonObject = new JSONObject();

        if (jsonSchema.has("properties")) {
            JsonNode propertiesNode = jsonSchema.get("properties");
            propertiesNode.fields().forEachRemaining(entry -> {
                String propertyName = entry.getKey();
                JsonNode propertySchema = entry.getValue();

                if (propertySchema.has("type")) {
                    String propertyType = propertySchema.get("type").asText();
                    switch (propertyType) {
                        case "string":
                            jsonObject.put(propertyName, faker.lorem().word());
                            break;
                        case "integer":
                            int minimum = propertySchema.has("minimum") ? propertySchema.get("minimum").asInt() : 0;
                            int maximum = propertySchema.has("maximum") ? propertySchema.get("maximum").asInt() : Integer.MAX_VALUE;
                            int randomInt = faker.random().nextInt(minimum, maximum);
                            jsonObject.put(propertyName, randomInt);
                            break;
                        // Add support for other data types as needed
                    }
                }
            });
        }

        if (jsonSchema.has("required")) {
            JsonNode requiredNode = jsonSchema.get("required");
            requiredNode.elements().forEachRemaining(requiredField -> {
                String fieldName = requiredField.asText();
                if (!jsonObject.has(fieldName)) {
                    jsonObject.put(fieldName, JSONObject.NULL);
                }
            });
        }

        return jsonObject;
    }
}
