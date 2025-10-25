package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class MongodbSourceCollectionConfig {

    @JsonProperty("database")
    private String database;

    @JsonProperty("collection")
    private String collection;

    @JsonProperty("schema")
    private TypeReference<List<Map<String, Object>>> schema;
}
