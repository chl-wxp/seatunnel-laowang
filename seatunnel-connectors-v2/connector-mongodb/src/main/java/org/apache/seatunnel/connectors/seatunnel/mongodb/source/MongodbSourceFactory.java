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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSourceCollectionConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class MongodbSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MongodbConfig.URI,
                        MongodbConfig.DATABASE,
                        MongodbConfig.COLLECTION,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        MongodbConfig.PROJECTION,
                        MongodbConfig.MATCH_QUERY,
                        MongodbConfig.SPLIT_SIZE,
                        MongodbConfig.SPLIT_KEY,
                        MongodbConfig.CURSOR_NO_TIMEOUT,
                        MongodbConfig.FETCH_SIZE,
                        MongodbConfig.MAX_TIME_MIN,
                        MongodbConfig.COLLECTION_LIST)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>>
            getSourceClass() {
        return MongodbSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        final ReadonlyConfig options = context.getOptions();
        Map<TablePath, CatalogTable> catalogTableMap = new HashMap<>();
        if (options.getOptional(MongodbConfig.COLLECTION_LIST).isPresent()) {
            final List<MongodbSourceCollectionConfig> mongodbSourceCollectionConfigs =
                    options.get(MongodbConfig.COLLECTION_LIST);
            mongodbSourceCollectionConfigs.forEach(
                    config -> {
                        final TablePath tablePath =
                                TablePath.of(config.getDatabase(), config.getCollection());
                        CatalogTable table;
                        if (config.getSchema() != null) {
                            table = CatalogTableUtil.buildWithConfig(options);
                        } else {
                            table = CatalogTableUtil.buildSimpleTextTable();
                        }
                        catalogTableMap.put(tablePath, renameCatalogTable(tablePath, table));
                    });
        } else {
            String database = options.get(MongodbConfig.DATABASE);
            String collection = options.get(MongodbConfig.COLLECTION);
            TablePath tablePath = TablePath.of(database, collection);
            CatalogTable table;
            if (options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                table = CatalogTableUtil.buildWithConfig(options);
            } else {
                table = CatalogTableUtil.buildSimpleTextTable();
            }
            catalogTableMap.put(tablePath, renameCatalogTable(tablePath, table));
        }
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new MongodbSource(catalogTableMap, options);
    }

    private CatalogTable renameCatalogTable(TablePath tablePath, CatalogTable catalogTable) {
        final TableIdentifier tableIdentifier = TableIdentifier.of(CONNECTOR_IDENTITY, tablePath);
        return CatalogTable.of(tableIdentifier, catalogTable);
    }
}
