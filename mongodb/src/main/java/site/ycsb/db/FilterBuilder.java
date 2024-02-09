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

import static com.mongodb.client.model.Filters.eqFull;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.and;

import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;

public final class FilterBuilder {
    public static Bson buildConcatenatedFilter(List<Comparison> filters) {
        // Bson[] bFilters = new Bson[filters.size()]; Arrays.asList(bFilters);
        List<Bson> lFilters = new ArrayList<>(filters.size());
        for(Comparison c : filters) {
            Comparison d = c;
            String fieldName = d.getFieldname();
            while(d.isSimpleNesting()) {
                d = d.getSimpleNesting();
                fieldName = fieldName + "." + d.getFieldname();
            }
            if(d.comparesStrings()) {
                lFilters.add(
                    FilterBuilder.buildStringFilter(
                        fieldName,
                        d.getOperator(),
                        d.getOperandAsString()
                ));
            } else if(d.comparesInts()) {
                lFilters.add(
                    FilterBuilder.buildIntFilter(
                        fieldName,
                        d.getOperator(),
                        d.getOperandAsInt()
                ));
            } else {
                throw new IllegalStateException();
            }
        }
        return and(lFilters);
    }

    public static Bson buildStringFilter(String fieldName, ComparisonOperator op, String operand) {
        switch (op) {
            case STRING_EQUAL:
                return eqFull(fieldName, operand);
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }

    public static Bson buildIntFilter(String fieldName, ComparisonOperator op, int operand) {
        switch (op) {
            case INT_LTE:
                return lte(fieldName, operand);
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }
    private FilterBuilder() {
        // no public constructor
    }
}
