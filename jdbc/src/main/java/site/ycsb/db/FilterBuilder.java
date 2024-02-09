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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

public final class FilterBuilder {
    
    private static java.util.Base64.Encoder encoder = java.util.Base64.getEncoder();

    static String buildConcatenatedPlaceholderSet(List<DatabaseField> fields) {
        return innerBuildConcatenatedSet(fields, true);
    }

    static String buildConcatenatedSet(List<DatabaseField> fields) {
        return innerBuildConcatenatedSet(fields, false);
    }

    private static String innerBuildConcatenatedSet(List<DatabaseField> fields, boolean placeholder) {
        List<String> parts = new ArrayList<>();
        for (DatabaseField value: fields) {
            DataWrapper w = value.getContent();
            if(w.isTerminal()) {
                if(w.isInteger()) {
                    parts.add(value.getFieldname() + " = " + (placeholder ? "?" : w.asInteger()));
                } else if(w.isLong()) {
                    parts.add(value.getFieldname() + " = " + (placeholder ? "?" : w.asLong()));
                } else if(w.isString()) {
                    if(placeholder) {
                        parts.add(value.getFieldname() + " = ? ");
                    } else {
                        parts.add(value.getFieldname() + " = '" + w.asString() + "'");
                    }
                } else {
                    // assuming this is an iterator
                    // which is the only remaining terminal
                    if(placeholder) {
                        parts.add(value.getFieldname() + " = ? ");
                    } else {
                        ByteIterator bi = w.asIterator();
                        bi.reset();
                        parts.add(value.getFieldname() + " = '" + encoder.encodeToString(bi.toArray()) + "'");
                    }
                }
            } else if(w.isArray()) {
                throw new UnsupportedOperationException("array type columns not supported (yet)");
            } else if(w.isNested()) {
                throw new UnsupportedOperationException("nested type columns not supported (yet)");
            } else {
                // do nothing; something broke that we ignore for now
            }
        }
        return concat(parts);
    }

    static int bindConcatenatedSet(PreparedStatement statement, int startIndex, List<DatabaseField> fields) throws SQLException {
        int currentIndex = startIndex;
        for (DatabaseField value: fields) {
            DataWrapper w = value.getContent();
            if(w.isTerminal()) {
                if(w.isInteger()) {
                    statement.setInt(currentIndex, w.asInteger());
                } else if(w.isLong()) {
                    statement.setLong(currentIndex, w.asLong());
                } else if(w.isString()) {
                    statement.setString(currentIndex, w.asString());
                } else {
                    ByteIterator bi = w.asIterator();
                    bi.reset();
                    statement.setString(currentIndex, encoder.encodeToString(bi.toArray()));
                    // assuming this is an iterator
                    // which is the only remaining terminal
                }
            } else if(w.isArray()) {
                throw new UnsupportedOperationException("array type columns not supported (yet)");
            } else if(w.isNested()) {
                throw new UnsupportedOperationException("nested type columns not supported (yet)");
            } else {
                // do nothing; something broke that we ignore for now
            }
            currentIndex++;
        }
        return currentIndex;
    }

    static int bindFilterValues(PreparedStatement statement, int startIndex, List<Comparison> filters) throws SQLException {
        int currentIndex = startIndex;
        for(Comparison c : filters) {
            if(c.isSimpleNesting()) {
                throw new IllegalArgumentException("nestings not supported for JDBC");
            }
            if(c.comparesStrings()) {
                statement.setString(currentIndex, c.getOperandAsString());
            } else if(c.comparesInts()) {
                statement.setInt(currentIndex, c.getOperandAsInt());
            } else {
                throw new IllegalStateException();
            }
            currentIndex++;
        }
        return currentIndex;
    }

    static String buildConcatenatedPlaceholderFilter(List<Comparison> filters) {
        return buildConcatenatedInnerFilter(filters, true);
    }

    static String buildConcatenatedFilter(List<Comparison> filters) {
        return buildConcatenatedInnerFilter(filters, false);
    }

    static String buildConcatenatedInnerFilter(List<Comparison> filters, boolean placeholder) {
        List<String> lFilters = new ArrayList<>(filters.size());
        for(Comparison c : filters) {
            Comparison d = c;
            String fieldName = d.getFieldname();
            if(d.isSimpleNesting()) {
                throw new IllegalArgumentException("nestings not supported for JDBC");
            }
            if(d.comparesStrings()) {
                lFilters.add(
                    FilterBuilder.buildStringFilter(
                        fieldName,
                        d.getOperator(),
                        placeholder ? null : d.getOperandAsString()
                ));
            } else if(d.comparesInts()) {
                lFilters.add(
                    FilterBuilder.buildIntFilter(
                        fieldName,
                        d.getOperator(),
                        placeholder ? null : d.getOperandAsInt()
                ));
            } else {
                throw new IllegalStateException();
            }
        }
        return and(lFilters);
    }

    static String buildStringFilter(String fieldName, ComparisonOperator op, String value) {
        String operand = value == null ? "? " : "'" + value + "' ";
        switch (op) {
            case STRING_EQUAL:
                return fieldName + " LIKE " + operand;
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }

    static String buildIntFilter(String fieldName, ComparisonOperator op, Integer value) {
        String operand = value == null ? "? " : value.toString();
        switch (op) {
            case INT_LTE:
                return fieldName + " <= " + operand;
            default:
                throw new IllegalArgumentException("no int operator");
        }
    }

    static String concat(List<String> args){
        String result = args.get(0);
        for(int i = 1; i < args.size(); i++) {
            result = result + " , " + args.get(i);
        }
        return result;
    }

    static String and(List<String> args){
        String result = args.get(0);
        for(int i = 1; i < args.size(); i++) {
            result = result + " AND " + args.get(i);
        }
        return result;
    }

    static void drainTypedResult(ResultSet result, Map<String, ByteIterator> resultBuffer) throws SQLException {
        final SchemaHolder schema = SchemaHolder.INSTANCE;
        for(SchemaColumn c : schema.getOrderedListOfColumns()) {
            String cName = c.getColumnName();
            if(c.getColumnType() == SchemaColumnType.TEXT) {
                String value = result.getString(cName);
                resultBuffer.put(cName, new StringByteIterator(value));
            } else if(c.getColumnType() == SchemaColumnType.LONG) {
                long value = result.getLong(cName);
                resultBuffer.put(cName, new NumericByteIterator(value));
            } else if(c.getColumnType() == SchemaColumnType.INT) {
                int value = result.getInt(cName);
                resultBuffer.put(cName, new NumericByteIterator(value));
            } else if(c.getColumnType() == SchemaColumnType.BYTES) {
                byte[] value = result.getBytes(cName);
                resultBuffer.put(cName, new StringByteIterator(new String(value)));
            } else {
                throw new IllegalArgumentException("not supported");
            }
        }
    }

    private FilterBuilder () {
        // no instances
    }
}
