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
package site.ycsb.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.IntNode;

import site.ycsb.db.DynamoDBClient.IndexDescriptor;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnKind;

public final class DynamoDBInitHelper {
    
  static List<IndexDescriptor> getIndexList(Properties props, int defaultReadCap, int defaultWriteCap) {
    String indexeslist = props.getProperty(DynamoDBClient.INDEX_LIST_PROPERTY);
    if(indexeslist == null) {
      return Collections.emptyList();
    }
    DynamoDBClient.LOGGER.info("raw index property: " + indexeslist);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = null;
    try {
      root = mapper.readTree(indexeslist);
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    DynamoDBClient.LOGGER.info("parsed index property: " + root.toString());
    if(!root.isArray()) {
      throw new IllegalArgumentException("index specification must be a JSON array");
    }
    ArrayNode array = (ArrayNode) root;
    if(array.size() == 0) {
      return Collections.emptyList();
    }
    DynamoDBClient.LOGGER.info("parsed index array with size: " + array.size());
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

      JsonNode hash = object.get("hashAttribute");
      if(hash != null && !hash.isTextual()){
        throw new IllegalArgumentException("index elements must be a JSON object. If 'hashAttribute' is set, it must be textual");
      } else if(hash != null) {
        desc.hashKeyAttribute = ((TextNode) hash).asText();
      }

      JsonNode sorts = object.get("sortAttributes");
      if(sorts != null && !sorts.isArray()) {
        ArrayNode sortArray = (ArrayNode) sorts;
        for(int j = 0; j < sortArray.size(); j++) {
          JsonNode sortEl = sortArray.get(j);
          if(sortEl == null || !sortEl.isTextual()) {
            throw new IllegalArgumentException("index elements must be a JSON object with 'sortAttributes' set as an array of strings");
          }
          desc.sortsKeyAttributes.add(((TextNode) sortEl).asText());
        }
      }

      JsonNode readCap = object.get("readCap");
      if(readCap == null || !readCap.isNumber()) {
        desc.readCap = defaultReadCap;
      } else {
        desc.readCap = ((IntNode) readCap).asInt();
      }

      JsonNode writeCap = object.get("writeCap");
      if(writeCap == null || !writeCap.isNumber()) {
        desc.writeCap = defaultWriteCap;
      } else {
        desc.writeCap = ((IntNode) writeCap).asInt();
      }

      DynamoDBClient.LOGGER.info("parsed array[" + i + "]: " + desc);
      retVal.add(desc);
    }
    return retVal;
  }

  private static AttributeDefinition attributeDefinitionForColumn(SchemaColumn c) {
    // fixme: consider nested columns
    if(c.getColumnKind() == SchemaColumnKind.SCALAR) {
        AttributeDefinition def = new AttributeDefinition();
        def.setAttributeName(c.getColumnName());
        switch(c.getColumnType()) {
            case BYTES:
                def.setAttributeType(ScalarAttributeType.B);
                break;
            case INT:
            case LONG:
                def.setAttributeType(ScalarAttributeType.N);
                break;
            case TEXT:
                def.setAttributeType(ScalarAttributeType.S);
                break;
            case CUSTOM:
            default:
                throw new IllegalArgumentException("not supported: " + c.getColumnType());
        }
        return def;
    } else if(c.getColumnKind() == SchemaColumnKind.NESTED) {
        if(c.getColumnName() == "airline") {
        return new AttributeDefinition()
          .withAttributeName("airline.alias")
          .withAttributeType("S");
        }
        return null;
    } else if(c.getColumnKind() == SchemaColumnKind.ARRAY) {
        return null;
    }
    throw new IllegalArgumentException("not supported: " + c.getColumnType());
  }

  static List<AttributeDefinition> getFullAttributeDefinitionList() {
    List<SchemaColumn> l = SchemaHolder.INSTANCE.getOrderedListOfColumns();
    List<AttributeDefinition> fields = new ArrayList<>();
    for(SchemaColumn c : l) {
        AttributeDefinition d = attributeDefinitionForColumn(c);
        if(d != null) fields.add(d);
    }
    return fields;
  }

  static CreateGlobalSecondaryIndexAction getCreateSecondaryIndexAction(IndexDescriptor idx) {
    // GlobalSecondaryIndex
    CreateGlobalSecondaryIndexAction action = new CreateGlobalSecondaryIndexAction();
    action.setIndexName(idx.name);
    action.setProjection(new Projection().withProjectionType("ALL"));
    List<KeySchemaElement> schema = new ArrayList<>();
    if(idx.hashKeyAttribute != null) {
        schema.add(new KeySchemaElement(idx.hashKeyAttribute, KeyType.HASH));
    }
    for(String s : idx.sortsKeyAttributes) {
        schema.add(new KeySchemaElement(s, KeyType.RANGE));
    }
    action.setKeySchema(schema);
    action.setProvisionedThroughput(
        new ProvisionedThroughput()
        .withReadCapacityUnits(Long.valueOf(idx.readCap))
        .withWriteCapacityUnits(Long.valueOf(idx.writeCap))
    );
    return action;
  }

    private DynamoDBInitHelper() {

    }
}
