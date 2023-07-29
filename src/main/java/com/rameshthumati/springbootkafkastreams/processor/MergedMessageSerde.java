package com.rameshthumati.springbootkafkastreams.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class MergedMessageSerde implements Serde<MergedMessage> {
    private final Serializer<MergedMessage> serializer;
    private final Deserializer<MergedMessage> deserializer;

    public MergedMessageSerde() {
        serializer = new MergedMessageSerializer();
        deserializer = new MergedMessageDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<MergedMessage> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MergedMessage> deserializer() {
        return deserializer;
    }

    private static class MergedMessageSerializer implements Serializer<MergedMessage> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, MergedMessage data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing MergedMessage: " + e.getMessage(), e);
            }
        }
    }

    private static class MergedMessageDeserializer implements Deserializer<MergedMessage> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public MergedMessage deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, MergedMessage.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing MergedMessage: " + e.getMessage(), e);
            }
        }
    }
}
