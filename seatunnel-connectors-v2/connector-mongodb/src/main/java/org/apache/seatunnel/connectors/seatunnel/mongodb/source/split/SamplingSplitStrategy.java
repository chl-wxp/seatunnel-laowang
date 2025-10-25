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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SamplingSplitStrategy implements MongoSplitStrategy, Serializable {

    private final Map<TablePath, MongodbClientProvider> clientProviderMap;

    private final String splitKey;

    private final BsonDocument matchQuery;

    private final BsonDocument projection;

    private final long samplesPerSplit;

    private final long sizePerSplit;

    SamplingSplitStrategy(
            Map<TablePath, MongodbClientProvider> clientProviderMap,
            String splitKey,
            BsonDocument matchQuery,
            BsonDocument projection,
            long samplesPerSplit,
            long sizePerSplit) {
        this.clientProviderMap = clientProviderMap;
        this.splitKey = splitKey;
        this.matchQuery = matchQuery;
        this.projection = projection;
        this.samplesPerSplit = samplesPerSplit;
        this.sizePerSplit = sizePerSplit;
    }

    @Override
    public List<MongoSplit> split() {
        Map<TablePath, ImmutablePair<Long, Long>> numAndAvgSizeMap = getDocumentNumAndAvgSize();
        List<MongoSplit> mongoSplitList = new ArrayList<>();
        numAndAvgSizeMap
                .keySet()
                .forEach(
                        tablePath -> {
                            final ImmutablePair<Long, Long> numAndAvgSize =
                                    numAndAvgSizeMap.get(tablePath);
                            long count = numAndAvgSize.getLeft();
                            long avgSize = numAndAvgSize.getRight();
                            // Handle the case when avgSize is 0 to prevent division by zero
                            if (avgSize <= 0) {
                                // If there are documents in the collection, return a single split
                                if (count > 0) {
                                    mongoSplitList.addAll(
                                            Lists.newArrayList(
                                                    MongoSplitUtils.createMongoSplit(
                                                            0,
                                                            matchQuery,
                                                            projection,
                                                            splitKey,
                                                            null,
                                                            null,
                                                            tablePath)));
                                } else {
                                    // If there are no documents, return an empty list
                                    mongoSplitList.addAll(Lists.newArrayList());
                                }
                            }
                            long numDocumentsPerSplit = sizePerSplit / avgSize;
                            int numSplits = (int) Math.ceil((double) count / numDocumentsPerSplit);
                            int numSamples = (int) Math.floor(samplesPerSplit * numSplits);
                            if (numSplits == 0) {
                                mongoSplitList.addAll(Lists.newArrayList());
                            }
                            if (numSplits == 1) {
                                mongoSplitList.addAll(
                                        Lists.newArrayList(
                                                MongoSplitUtils.createMongoSplit(
                                                        0,
                                                        matchQuery,
                                                        projection,
                                                        splitKey,
                                                        null,
                                                        null,
                                                        tablePath)));
                            }
                            List<BsonDocument> samples =
                                    sampleCollection(numSamples, clientProviderMap.get(tablePath));
                            if (samples.isEmpty()) {
                                mongoSplitList.addAll(Lists.newArrayList());
                            }
                            List<Object> rightBoundaries =
                                    IntStream.range(0, samples.size())
                                            .filter(
                                                    i ->
                                                            i % samplesPerSplit == 0
                                                                    || !matchQuery.isEmpty()
                                                                            && i == count - 1)
                                            .mapToObj(i -> samples.get(i).get(splitKey))
                                            .collect(Collectors.toList());

                            mongoSplitList.addAll(
                                    createSplits(splitKey, rightBoundaries, tablePath));
                        });
        return mongoSplitList;
    }

    @VisibleForTesting
    protected Map<TablePath, ImmutablePair<Long, Long>> getDocumentNumAndAvgSize() {
        Map<TablePath, ImmutablePair<Long, Long>> immutablePairMap = new HashMap<>();
        clientProviderMap
                .keySet()
                .forEach(
                        tablePath -> {
                            final MongodbClientProvider clientProvider =
                                    clientProviderMap.get(tablePath);
                            String collectionName =
                                    clientProvider
                                            .getDefaultCollection()
                                            .getNamespace()
                                            .getCollectionName();
                            BsonDocument statsCmd =
                                    new BsonDocument("collStats", new BsonString(collectionName));
                            Document res = clientProvider.getDefaultDatabase().runCommand(statsCmd);
                            Object count = res.get("count");
                            // fix issue https://github.com/apache/seatunnel/issues/7575
                            long total =
                                    Optional.ofNullable(count)
                                            .map(
                                                    v ->
                                                            new BigDecimal(String.valueOf(count))
                                                                    .longValue())
                                            .orElse(0L);
                            Object avgDocumentBytes = res.get("avgObjSize");
                            long avgObjSize =
                                    Optional.ofNullable(avgDocumentBytes)
                                            .map(
                                                    docBytes -> {
                                                        if (docBytes instanceof Integer) {
                                                            return ((Integer) docBytes).longValue();
                                                        } else if (docBytes instanceof Double) {
                                                            return ((Double) docBytes).longValue();
                                                        } else {
                                                            return 0L;
                                                        }
                                                    })
                                            .orElse(0L);

                            if (matchQuery == null || matchQuery.isEmpty()) {
                                immutablePairMap.put(
                                        tablePath, ImmutablePair.of(total, avgObjSize));
                            } else {
                                immutablePairMap.put(
                                        tablePath,
                                        ImmutablePair.of(
                                                clientProvider
                                                        .getDefaultCollection()
                                                        .countDocuments(matchQuery),
                                                avgObjSize));
                            }
                        });
        return immutablePairMap;
    }

    private List<BsonDocument> sampleCollection(
            int numSamples, MongodbClientProvider clientProvider) {
        return clientProvider
                .getDefaultCollection()
                .aggregate(
                        Lists.newArrayList(
                                Aggregates.match(matchQuery),
                                Aggregates.sample(numSamples),
                                Aggregates.project(Projections.include(splitKey)),
                                Aggregates.sort(Sorts.ascending(splitKey))))
                .allowDiskUse(true)
                .into(Lists.newArrayList());
    }

    private List<MongoSplit> createSplits(
            String splitKey, List<Object> rightBoundaries, TablePath tablePath) {
        if (rightBoundaries.size() == 0) {
            return Collections.emptyList();
        }

        List<MongoSplit> splits =
                IntStream.range(0, rightBoundaries.size())
                        .mapToObj(
                                index -> {
                                    Object min = index > 0 ? rightBoundaries.get(index - 1) : null;
                                    return MongoSplitUtils.createMongoSplit(
                                            index,
                                            matchQuery,
                                            projection,
                                            splitKey,
                                            min,
                                            rightBoundaries.get(index),
                                            tablePath);
                                })
                        .collect(Collectors.toList());

        Object lastBoundary = rightBoundaries.get(rightBoundaries.size() - 1);
        splits.add(
                MongoSplitUtils.createMongoSplit(
                        splits.size(),
                        matchQuery,
                        projection,
                        splitKey,
                        lastBoundary,
                        null,
                        tablePath));
        return splits;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<TablePath, MongodbClientProvider> clientProviderMap;

        private String splitKey;

        private BsonDocument matchQuery;

        private BsonDocument projection;

        private long samplesPerSplit;

        private long sizePerSplit;

        private static final BsonDocument EMPTY_MATCH_QUERY = new BsonDocument();

        private static final BsonDocument EMPTY_PROJECTION = new BsonDocument();

        private static final long DEFAULT_SAMPLES_PER_SPLIT = 10;

        Builder() {
            this.clientProviderMap = null;
            this.matchQuery = EMPTY_MATCH_QUERY;
            this.projection = EMPTY_PROJECTION;
            this.samplesPerSplit = DEFAULT_SAMPLES_PER_SPLIT;
        }

        public Builder setClientProvider(Map<TablePath, MongodbClientProvider> clientProviderMap) {
            this.clientProviderMap = clientProviderMap;
            return this;
        }

        public Builder setSplitKey(String splitKey) {
            this.splitKey = splitKey;
            return this;
        }

        public Builder setMatchQuery(BsonDocument matchQuery) {
            this.matchQuery = matchQuery;
            return this;
        }

        public Builder setProjection(BsonDocument projection) {
            this.projection = projection;
            return this;
        }

        public Builder setSamplesPerSplit(long samplesPerSplit) {
            this.samplesPerSplit = samplesPerSplit;
            return this;
        }

        public Builder setSizePerSplit(long sizePerSplit) {
            this.sizePerSplit = sizePerSplit;
            return this;
        }

        public SamplingSplitStrategy build() {
            Preconditions.checkNotNull(clientProviderMap);
            return new SamplingSplitStrategy(
                    clientProviderMap,
                    splitKey,
                    matchQuery,
                    projection,
                    samplesPerSplit,
                    sizePerSplit);
        }
    }
}
