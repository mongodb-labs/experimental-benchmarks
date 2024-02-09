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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

public final class DynamoDBQueryParameterHelper {

  private static AttributeValue fillAttribute(Object o) {
    AttributeValue content = new AttributeValue();
    if(String.class.isInstance(o)) {
        content.setS((String) o);
    } else if(
        Integer.class.isInstance(o) || int.class.isInstance(o) ||
        Long.class.isInstance(o) || long.class.isInstance(o) ||
        Double.class.isInstance(o) || double.class.isInstance(o) ||
        Float.class.isInstance(o) || float.class.isInstance(o)) {
          content.setN(o.toString());
    } else {
      throw new IllegalArgumentException("what? " + o);
    }
    return content;
  }
  private static void fillDocument(DatabaseField field, Map<String, AttributeValue> toInsert) {
    DataWrapper wrapper = field.getContent();
    AttributeValue content;
    if(wrapper.isTerminal()){
      content = new AttributeValue();
      if(wrapper.isInteger() || wrapper.isLong()) {
        content.setN(wrapper.asObject().toString());
      } else if(wrapper.isString()) {
        content.setS(wrapper.asString());
      } else {
        // as iterator
        Object o = field.getContent().asObject();
        byte[] b = (byte[]) o;
        content.setS(new String(b));
      }
    } else if (wrapper.isArray()) {
      final List<Object> oa = (List<Object>) wrapper.asObject();
      List<AttributeValue> elements = new ArrayList<>(oa.size());
      for(int i = 0; i < oa.size(); i++) {
        // this WILL BREAK if content is a nested
        // document within the array
        elements.add(fillAttribute(oa.get(i)));
      }
      content = new AttributeValue();
      content.setL(elements);
    } else if(wrapper.isNested()) {
        content = new AttributeValue();
        Map<String, AttributeValue> inner = new HashMap<>();
        List<DatabaseField> innerFields = wrapper.asNested();
        for(DatabaseField iF : innerFields) {
          fillDocument(iF, inner);
        }
        content.setM(inner);
      } else {
        throw new IllegalStateException("neither terminal, nor array, nor nested");
      }
      toInsert.put(
        field.getFieldname(),
        content
      );
  }

  static Map<String, AttributeValue> createTypedAttributes(List<DatabaseField> values) {
    Map<String, AttributeValue> toInsert = new HashMap<>(values.size() + 1);
    for (DatabaseField field : values) {
      fillDocument(field, toInsert);
    }
    return toInsert;
  }

  static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), new AttributeValue(val.getValue().toString()));
    }
    return attributes;
  }

  static HashMap<String, ByteIterator> extractResultFromItem(Item item) {
    System.err.println("extracting item: " + item.toJSONPretty());
    return new HashMap<>();
  }

  static HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());

    for (Entry<String, AttributeValue> attr : item.entrySet()) {
      if (DynamoDBClient.LOGGER.isDebugEnabled()) {
        DynamoDBClient.LOGGER.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()));
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().getS()));
    }
    return rItems;
  }

  private DynamoDBQueryParameterHelper() {
    // random
  } 
}
