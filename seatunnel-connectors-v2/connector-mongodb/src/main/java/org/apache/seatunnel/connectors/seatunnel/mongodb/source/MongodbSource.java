/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentRowDataDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.config.MongodbReadOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator.MongodbSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader.MongodbReader;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.SamplingSplitStrategy;

import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>,
                SupportParallelism,
                SupportColumnProjection {

    private static final long serialVersionUID = 1L;

    private final Map<TablePath, CatalogTable> catalogTables;
    private final ReadonlyConfig options;

    public MongodbSource(Map<TablePath, CatalogTable> catalogTables, ReadonlyConfig options) {
        this.catalogTables = catalogTables;
        this.options = options;
    }

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(catalogTables.values());
    }

    @Override
    public SourceReader<SeaTunnelRow, MongoSplit> createReader(SourceReader.Context readerContext) {
        Map<TablePath, DocumentDeserializer<SeaTunnelRow>> documentDeserializerMap =
                new LinkedHashMap<>();
        catalogTables
                .keySet()
                .forEach(
                        tablePath -> {
                            documentDeserializerMap.put(
                                    tablePath,
                                    createDeserializer(
                                            options,
                                            catalogTables.get(tablePath).getSeaTunnelRowType()));
                        });
        return new MongodbReader(
                readerContext,
                crateClientProvider(options),
                documentDeserializerMap,
                createMongodbReadOptions(options));
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> createEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext) {
        Map<TablePath, MongodbClientProvider> clientProvider = crateClientProvider(options);
        return new MongodbSplitEnumerator(
                enumeratorContext, clientProvider, createSplitStrategy(options, clientProvider));
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext,
            ArrayList<MongoSplit> checkpointState) {
        Map<TablePath, MongodbClientProvider> clientProviderMap = crateClientProvider(options);
        return new MongodbSplitEnumerator(
                enumeratorContext,
                clientProviderMap,
                createSplitStrategy(options, clientProviderMap),
                checkpointState);
    }

    private Map<TablePath, MongodbClientProvider> crateClientProvider(ReadonlyConfig config) {
        Map<TablePath, MongodbClientProvider> mongodbClientProviderMap = new HashMap<>();
        catalogTables
                .keySet()
                .forEach(
                        tablePath -> {
                            final MongodbClientProvider mongodbClientProvider =
                                    MongodbCollectionProvider.builder()
                                            .connectionString(config.get(MongodbConfig.URI))
                                            .database(tablePath.getDatabaseName())
                                            .collection(tablePath.getTableName())
                                            .build();
                            mongodbClientProviderMap.put(tablePath, mongodbClientProvider);
                        });
        return mongodbClientProviderMap;
    }

    private DocumentRowDataDeserializer createDeserializer(
            ReadonlyConfig config, SeaTunnelRowType rowType) {
        return new DocumentRowDataDeserializer(
                rowType.getFieldNames(), rowType, config.get(MongodbConfig.FLAT_SYNC_STRING));
    }

    private MongoSplitStrategy createSplitStrategy(
            ReadonlyConfig config, Map<TablePath, MongodbClientProvider> mongodbClientProviderMap) {
        SamplingSplitStrategy.Builder splitStrategyBuilder = SamplingSplitStrategy.builder();
        splitStrategyBuilder.setSplitKey(config.get(MongodbConfig.SPLIT_KEY));
        splitStrategyBuilder.setSizePerSplit(config.get(MongodbConfig.SPLIT_SIZE));
        config.getOptional(MongodbConfig.MATCH_QUERY)
                .ifPresent(s -> splitStrategyBuilder.setMatchQuery(BsonDocument.parse(s)));
        config.getOptional(MongodbConfig.PROJECTION)
                .ifPresent(s -> splitStrategyBuilder.setProjection(BsonDocument.parse(s)));
        return splitStrategyBuilder.setClientProvider(mongodbClientProviderMap).build();
    }

    private MongodbReadOptions createMongodbReadOptions(ReadonlyConfig config) {
        MongodbReadOptions.MongoReadOptionsBuilder mongoReadOptionsBuilder =
                MongodbReadOptions.builder();
        mongoReadOptionsBuilder.setMaxTimeMS(config.get(MongodbConfig.MAX_TIME_MIN));
        mongoReadOptionsBuilder.setFetchSize(config.get(MongodbConfig.FETCH_SIZE));
        mongoReadOptionsBuilder.setNoCursorTimeout(config.get(MongodbConfig.CURSOR_NO_TIMEOUT));
        return mongoReadOptionsBuilder.build();
    }
}
