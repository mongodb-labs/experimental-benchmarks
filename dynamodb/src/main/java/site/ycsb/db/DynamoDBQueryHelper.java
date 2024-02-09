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

import java.util.List;

import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;

public final class DynamoDBQueryHelper {
    
    private static void bindQueryString(ValueMap m, Comparison c) {
        Comparison d = c;
        String fieldName = c.getFieldname();
        while(d.isSimpleNesting()) {
            d = d.getSimpleNesting();
            fieldName = fieldName + "." + d.getFieldname();
        }
        if(d.comparesInts()) {
            m.withInt(":v_"+ fieldName, d.getOperandAsInt());
        } else if(d.comparesStrings()) {
            m.withString(":v_"+ fieldName, d.getOperandAsString());
        } else {
            throw new IllegalArgumentException("unknown: " + c);
        }
    }

    private static String buildQueryString(Comparison c) {
        Comparison d = c;
        String fieldName = c.getFieldname();
        while(d.isSimpleNesting()) {
            d = d.getSimpleNesting();
            fieldName = fieldName + "." + d.getFieldname();
        }
        if(d.comparesInts()) {
            ComparisonOperator co = d.getOperator();
            if(co == ComparisonOperator.INT_LTE) {
                return fieldName + " <= " + ":v_"+ fieldName;
            } else {
                throw new IllegalArgumentException("unknown: " + co);
            }
        } else if(d.comparesStrings()) {
            ComparisonOperator co = d.getOperator();
            if(co == ComparisonOperator.STRING_EQUAL) {
                return fieldName + " = " + ":v_"+ fieldName;
            } else {
                throw new IllegalArgumentException("unknown: " + co);
            }
        } else {
                throw new IllegalArgumentException("unknown: " + c);
        }
    }

    static void bindPreparedQuery(QuerySpec query, List<Comparison> filters) {
        ValueMap m = new ValueMap();
        for(Comparison c : filters) {
            bindQueryString(m, c);
        }
        query.withValueMap(m);
    }

    static void buildPreparedQuery(QuerySpec query, String indexField, List<Comparison> filters) {
        query.withMaxResultSize(1);
        for(Comparison c : filters) {
            if(indexField.equals(c.getFieldname())) {
                query.withKeyConditionExpression(buildQueryString(c));
            } else {
                String f = query.getFilterExpression();
                String g = buildQueryString(c);
                String h = f == null ? g : f + " AND " + g;
                query.withFilterExpression(h);
            }
        }
    }

    private DynamoDBQueryHelper() {

    }
}
