//package com.rameshthumati.springbootkafkastreams.processor;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.kstream.Consumed;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.Produced;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//@Component
//public class EventStreamProcessor {
//
//    @Autowired
//    private StreamsBuilder streamsBuilder;
//
//    @PostConstruct
//    public void streamTopology() {
//        // Stream from topic.metadata.results
//        KStream<String, String> metadataStream = streamsBuilder.stream("topic.metadata.results", Consumed.with(Serdes.String(), Serdes.String()));
//
//        // Stream from topic-inference-results
//        KStream<String, String> inferenceStream = streamsBuilder.stream("topic-inference-results", Consumed.with(Serdes.String(), Serdes.String()));
//
//        // Merge the streams based on the "transaction_id"
//        KStream<String, String> mergedStream = metadataStream.merge(inferenceStream);
//
//        // Group by the "transaction_id" key and reduce to merge the messages
//        KStream<String, String> finalStream = mergedStream
//                .groupByKey()
//                .reduce(this::mergeMessages)
//                .toStream();
//
//        // Print the merged messages (optional, for debugging)
//        finalStream.peek((key, value) -> System.out.println("Merged Message : " + value));
//
//        // Publish the merged messages to the topic-metadata-inference-join topic
//        finalStream.to("topic-metadata-inference-join", Produced.with(Serdes.String(), Serdes.String()));
//    }
//
//    // Merge method to combine the messages based on "transaction_id"
//    private String mergeMessages(String metadataValue, String inferenceValue) {
//        // Parse JSON strings and merge the messages
//        try {
//            ObjectMapper mapper = new ObjectMapper();
//            JsonNode metadataJson = mapper.readTree(metadataValue);
//            JsonNode inferenceJson = mapper.readTree(inferenceValue);
//
//            ObjectNode mergedJson = mapper.createObjectNode();
//            mergedJson.put("transaction_id", metadataJson.get("transaction_id").asText());
//            mergedJson.put("metadata", metadataJson.get("metadata").asText());
//            mergedJson.put("inference", inferenceJson.get("inference").asText());
//
//            return mergedJson.toString();
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }
//}
