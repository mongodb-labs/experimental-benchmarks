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

import java.util.List;
import java.util.function.Function;

import org.omg.CORBA.Bounds;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

public final class FilterBuilder {
    
    static int bindFilter(int startIndex, BoundStatement bound, List<Comparison> filters) {
        int currentIndex = startIndex;
        for(Comparison c : filters){
            if(c.isSimpleNesting()) {
                throw new IllegalArgumentException("nestings are not supported for scylla.");
            }
            if(c.comparesStrings()) {
                bound.setString(currentIndex, c.getOperandAsString());
            } else if(c.comparesInts()) {
                bound.setInt(currentIndex, c.getOperandAsInt());
            } else {
                throw new IllegalStateException();
            }
            currentIndex++;
        }
        // this is the next index
        return currentIndex;
    }

    static void bindInnerFilter(List<Comparison> filters, boolean usePlaceholder, Function<Clause,Boolean> f) {
        for(Comparison c : filters) {
            if(c.isSimpleNesting()) {
                throw new IllegalArgumentException("nestings are not supported for scylla.");
            }
            if(c.comparesStrings()) {
                f.apply(
                    buildFilterClause(c.getFieldname(), c.getOperator(), usePlaceholder ? QueryBuilder.bindMarker() : c.getOperandAsString())
                );
            } else if(c.comparesInts()) {
                f.apply(
                    buildFilterClause(c.getFieldname(), c.getOperator(), usePlaceholder ? QueryBuilder.bindMarker() : c.getOperandAsInt())
                );
            } else {
                throw new IllegalStateException();
            }
        }
    }
    static int bindPreparedAssignments(BoundStatement bound, List<DatabaseField> fields, int startIndex) {
        int currentIndex = startIndex;
        for(DatabaseField field : fields){
            DataWrapper wrapper = field.getContent();
            if(wrapper.isTerminal()) {
                if(wrapper.isInteger()) {
                    bound.setInt(currentIndex, field.getContent().asInteger());
                } else if(wrapper.isLong()) {
                    bound.setLong(currentIndex, field.getContent().asLong());
                } else if(wrapper.isString()) {
                    bound.setString(currentIndex, field.getContent().asString());
                }  else {
                    // assuming this is an iterator
                    // which is the only remaining terminal
                    Object o = field.getContent().asObject();
                    byte[] b = (byte[]) o;
                    bound.setString(currentIndex, new String(b));
                }
            } else if(wrapper.isNested()) {
                throw new IllegalArgumentException("setting nested elements is currently not supported");
            } else if(wrapper.isArray()) {
                throw new IllegalArgumentException("setting arrays or array content is currently not supported");
            } else {
                throw new IllegalStateException("neither terminal, nor array, nor nested");
            }
            currentIndex++;
        }
        return currentIndex;
    }
    static void buildPreparedAssignments(List<DatabaseField> fields, Assignments assignments) {
        for(DatabaseField field : fields){
            assignments.and(
                buildTypedAssignment(field, true)
            );
        }
    }
    static void buildAssignments(List<DatabaseField> fields, Assignments assignments) {
        for(DatabaseField field : fields){
            assignments.and(
                buildTypedAssignment(field, false)
            );
        }
    }
    private static Assignment buildTypedAssignment(DatabaseField field, boolean usePlaceholder) {
        DataWrapper wrapper = field.getContent();
        if(wrapper.isTerminal()) {
            final String fieldName = field.getFieldname();
            if(wrapper.isInteger()) {
                return QueryBuilder.set(fieldName, usePlaceholder ? QueryBuilder.bindMarker() : field.getContent().asInteger());
            }
            if(wrapper.isLong()) {
                return QueryBuilder.set(fieldName, usePlaceholder ? QueryBuilder.bindMarker() : field.getContent().asLong());
            } else if(wrapper.isString()) {
                return QueryBuilder.set(fieldName, usePlaceholder ? QueryBuilder.bindMarker() : field.getContent().asString());
            }  else {
                // assuming this is an iterator
                // which is the only remaining terminal
                if(usePlaceholder) {
                    return QueryBuilder.set(fieldName, QueryBuilder.bindMarker());
                } else {
                    Object o = field.getContent().asObject();
                    byte[] b = (byte[]) o;
                    return QueryBuilder.set(fieldName, new String(b));
                }
            }
        } else if(wrapper.isNested()) {
            throw new IllegalArgumentException("setting nested elements is currently not supported");
        } else if(wrapper.isArray()) {
            throw new IllegalArgumentException("setting arrays or array content is currently not supported");
        } else {
            throw new IllegalStateException("neither terminal, nor array, nor nested");
        }
    }

    static void buildPreparedFilter(List<Comparison> filters, Select.Where where) {
        buildInnerFilter(filters, true , (Clause c) -> {
            where.and(c);
            return Boolean.TRUE;
        });
    }

    static void buildFilter(List<Comparison> filters, Update.Where where) {
        buildInnerFilter(filters, false, (Clause c) -> {
            where.and(c);
            return Boolean.TRUE;
        });
    }
    static void buildFilter(List<Comparison> filters, Select.Where where) {
        buildInnerFilter(filters,  false, (Clause c) -> {
            where.and(c);
            return Boolean.TRUE;
        });
    }

    static void buildInnerFilter(List<Comparison> filters, boolean usePlaceholder, Function<Clause,Boolean> f) {
        for(Comparison c : filters) {
            if(c.isSimpleNesting()) {
                throw new IllegalArgumentException("nestings are not supported for scylla.");
            }
            if(c.comparesStrings()) {
                f.apply(
                    buildFilterClause(c.getFieldname(), c.getOperator(), usePlaceholder ? QueryBuilder.bindMarker() : c.getOperandAsString())
                );
            } else if(c.comparesInts()) {
                f.apply(
                    buildFilterClause(c.getFieldname(), c.getOperator(), usePlaceholder ? QueryBuilder.bindMarker() : c.getOperandAsInt())
                );
            } else {
                throw new IllegalStateException();
            }
        }
    }

    private static Clause buildFilterClause(String fieldName, ComparisonOperator op, Object operand) {
        switch (op) {
            case STRING_EQUAL:
                return QueryBuilder.like(fieldName, operand);
            case INT_LTE:
                return QueryBuilder.lte(fieldName, operand);
            default:
                throw new IllegalArgumentException("operator not supported");
        }
    }

    private FilterBuilder() {
        // empty
    }
}
