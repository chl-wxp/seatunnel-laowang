package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MongodbSourceCollectionConfig {

    @JsonProperty("database")
    private String database;

    @JsonProperty("collection")
    private String collection;

    @JsonProperty("schema")
    private Map<String, Object> schema;
}
