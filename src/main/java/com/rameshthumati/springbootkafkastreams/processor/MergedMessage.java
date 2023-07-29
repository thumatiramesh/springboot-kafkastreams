package com.rameshthumati.springbootkafkastreams.processor;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedMessage {
    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("metadata")
    private String metadata;

    @JsonProperty("inference")
    private String inference;

    public MergedMessage() {
        // Empty constructor for Jackson deserialization
    }

    public MergedMessage(String transactionId, String metadata, String inference) {
        this.transactionId = transactionId;
        this.metadata = metadata;
        this.inference = inference;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public String getInference() {
        return inference;
    }

    public void setInference(String inference) {
        this.inference = inference;
    }
}
