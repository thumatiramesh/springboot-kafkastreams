package com.rameshthumati.springbootkafkastreams.processor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class MessageSubStringProcessor {

    @Autowired
    private StreamsBuilder streamsBuilder;

    static ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    @PostConstruct
    public void streamTopology() {
        KStream<String, String> kStreamMetadata = streamsBuilder.stream("topic.metadata.results", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> kStreamInference = streamsBuilder.stream("topic-inference-results", Consumed.with(Serdes.String(), Serdes.String()));

        // Process metadata stream and convert to KStream<String, MergedMessage>
        KStream<String, MergedMessage> kStreamMerged = kStreamMetadata
                .filter((key, value) -> !value.isBlank())
                .mapValues((k, v) -> {
                    try {
                        v = v.trim();
                        if (v.startsWith("\"") && v.endsWith("\"")) {
                            v = v.substring(1, v.length() - 1);
                        }
                        MergedMessage mm = objectMapper.readValue(v.replace("\\n", "").replace("\\\"", "\""), MergedMessage.class);
                        return mm;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                });

        // Process inference stream and convert to KStream<String, MergedMessage>
        KStream<String, MergedMessage> kStreamMergedInference = kStreamInference
                .filter((key, value) -> !value.isBlank())
                .mapValues((k, v) -> {
                    try {
                        v = v.trim();
                        if (v.startsWith("\"") && v.endsWith("\"")) {
                            v = v.substring(1, v.length() - 1);
                        }
                        MergedMessage mm = objectMapper.readValue(v.replace("\\n", "\n").replace("\\\"", "\""), MergedMessage.class);
                        return mm;
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                });

        // Merge both streams and perform the merging logic
        KStream<String, MergedMessage> mergedStream = kStreamMerged
                .merge(kStreamMergedInference)
                .groupByKey()
                .reduce((v1, v2) -> {
                    String inference = v2.getInference() != null ? v2.getInference() : v1.getInference();
                    return new MergedMessage(v1.getTransactionId(), v1.getMetadata(), inference);
                }, Materialized.with(Serdes.String(), new MergedMessageSerde()))
                .toStream();

        // Convert the merged stream to JSON strings and publish to the output topic
        mergedStream
                .mapValues(value -> {
                    try {
                        return objectMapper.writeValueAsString(value);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .peek((k, v) -> System.out.println("Merged Message: " + v))
                .to("topic-metadata-inference-join", Produced.with(Serdes.String(), Serdes.String()));
    }
}

