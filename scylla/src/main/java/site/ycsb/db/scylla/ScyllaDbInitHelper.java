/*
 * Copyright 2023-2024 benchANT GmbH. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package site.ycsb.db.scylla;

import static site.ycsb.db.scylla.ScyllaDbConstants.REPLICATION_DEGREE_DEFAULT;
import static site.ycsb.db.scylla.ScyllaDbConstants.REPLICATION_DEGREE_PROPERTY;
import static site.ycsb.db.scylla.ScyllaDbConstants.REPLICATION_STRATEGY_CLASS_DEFAULT;
import static site.ycsb.db.scylla.ScyllaDbConstants.REPLICATION_STRATEGY_CLASS_PROPERTY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import site.ycsb.db.scylla.ScyllaCQLClient.IndexDescriptor;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnKind;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;

public final class ScyllaDbInitHelper {
    
    private static final SchemaHolder schema = SchemaHolder.INSTANCE;

    static Session createKeyspaceAndSchema(Properties props, String keyspace, Cluster cluster) {
      String tableName = props.getProperty(CoreConstants.TABLENAME_PROPERTY, CoreConstants.TABLENAME_PROPERTY_DEFAULT);
      Session session = cluster.connect();
      Map<String,Object> replication = new HashMap<>();
      replication.put("class", props.getProperty(REPLICATION_STRATEGY_CLASS_PROPERTY, REPLICATION_STRATEGY_CLASS_DEFAULT));
      replication.put("replication_factor", props.getProperty(REPLICATION_DEGREE_PROPERTY, REPLICATION_DEGREE_DEFAULT));
      Statement stmt = SchemaBuilder.createKeyspace(keyspace).ifNotExists().with()
        .durableWrites(true).replication(replication).enableTracing();
      // REPLICATION = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': 3}
      final String s = stmt.toString();
      System.err.println(s);
      session.execute(stmt);
      
      
      createTable(keyspace, tableName, session);
      session.close();
      return cluster.connect(keyspace);
    }

    private static void createTable(String keyspace, String tableName, Session session) {
      Create create = SchemaBuilder.createTable(keyspace, tableName);
      create.addPartitionKey(ScyllaDbConstants.YCSB_KEY, DataType.text());
      for(SchemaColumn column : schema.getOrderedListOfColumns()) {
        if(column.getColumnKind() == SchemaColumnKind.SCALAR) {
            if(column.getColumnType() == SchemaColumnType.TEXT)
              create.addColumn(column.getColumnName(), DataType.text());
            else if( column.getColumnType() == SchemaColumnType.INT)
              create.addColumn(column.getColumnName(), DataType.cint());
            else if( column.getColumnType() == SchemaColumnType.LONG)
              create.addColumn(column.getColumnName(), DataType.bigint());
        } else if(column.getColumnKind() == SchemaColumnKind.ARRAY) {
            throw new InvalidTypeException ("array column types currently not supported");
        } else if(column.getColumnKind() == SchemaColumnKind.NESTED) {
            throw new InvalidTypeException ("nested column types currently not supported");
        } else {
            throw new InvalidTypeException("other column types currently not supported");
        }
      }
      ResultSet rs = session.execute(create);
    }

  static List<IndexDescriptor> getIndexList(Properties props) {
    String indexeslist = props.getProperty(ScyllaDbConstants.INDEX_LIST_PROPERTY);
    if(indexeslist == null) {
      return Collections.emptyList();
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = null;
    try {
      root = mapper.readTree(indexeslist);
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    if(!root.isArray()) {
      throw new IllegalArgumentException("index specification must be a JSON array");
    }
    ArrayNode array = (ArrayNode) root;
    if(array.size() == 0) {
      return Collections.emptyList();
    }
    List<IndexDescriptor> retVal = new ArrayList<>();
    for(int i = 0; i < array.size(); i++) {
      JsonNode el = array.get(i);
      if(!el.isObject()) {
        throw new IllegalArgumentException("index elements must be a JSON object");
      }

      IndexDescriptor desc = new IndexDescriptor();
      ObjectNode object = (ObjectNode) el;
      JsonNode name = object.get("name");
      if(name == null || !name.isTextual()) {
        throw new IllegalArgumentException("index elements must be a JSON object with 'name' of type string");
      }
      desc.name = ((TextNode) name).asText();

      JsonNode sorts = object.get("columns");
      if(sorts == null || !sorts.isArray()) {
        throw new IllegalArgumentException("index elements must be a JSON object with 'columns' set as an array of strings");
      }
      ArrayNode columnsArray = (ArrayNode) sorts;
      for(int j = 0; j < columnsArray.size(); j++) {
        JsonNode sortEl = columnsArray.get(j);
        if(sortEl == null || !sortEl.isTextual()) {
          throw new IllegalArgumentException("index elements must be a JSON object with 'sortAttributes' set as an array of strings");
        }
        desc.columnNames.add(((TextNode) sortEl).asText());
      }
      retVal.add(desc);
    }
    return retVal;
  }

    private ScyllaDbInitHelper() {
        // empty
    }
}
