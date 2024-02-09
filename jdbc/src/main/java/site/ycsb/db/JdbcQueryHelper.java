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
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

public final class JdbcQueryHelper {
    
    static int bindFindOneStatement(PreparedStatement statement, List<Comparison> filters, Set<String> fields) throws SQLException {
        int currentIndex = 1;
        if(fields != null) {
            throw new IllegalStateException("reading specific fields currently not supported by this driver");
        }
        return FilterBuilder.bindFilterValues(statement, currentIndex, filters);
    }

    static String createFindOneStatement(String tablename, List<Comparison> filters, Set<String> fields) {
        if(fields != null) {
            throw new IllegalStateException("reading specific fields currently not supported by this driver");
        }
        final String template = "SELECT * FROM %1$s WHERE %2$s LIMIT 1";
        String filterString = FilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        return String.format(template, tablename, filterString);
    }

    static int bindUpdateOneStatement(PreparedStatement statement, List<Comparison> filters, List<DatabaseField> fields) throws SQLException {
        int currentIndex = 1;
        currentIndex = FilterBuilder.bindConcatenatedSet(statement, currentIndex, fields);
        return FilterBuilder.bindFilterValues(statement, currentIndex, filters);
    }

    static String createUpdateOneStatement(String tablename, List<Comparison> filters, List<DatabaseField> fields) {
        final String template = "UPDATE %1$s SET %2$s WHERE %3$s = (%4$s)";
        // https://dba.stackexchange.com/questions/69471/postgres-update-limit-1
        final String innerTemplate = "SELECT %1$s from %2$s WHERE %3$s LIMIT 1 FOR UPDATE SKIP LOCKED";
        
        String setString = FilterBuilder.buildConcatenatedPlaceholderSet(fields);
        String filterString = FilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        final String inner = String.format(innerTemplate, JdbcDBConstants.PRIMARY_KEY, tablename, filterString);
        return String.format(template, tablename, setString, JdbcDBConstants.PRIMARY_KEY, inner);
    }

    private JdbcQueryHelper() {

    }
}
