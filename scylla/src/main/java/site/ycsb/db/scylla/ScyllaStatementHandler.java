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

import static site.ycsb.db.scylla.ScyllaDbConstants.YCSB_KEY;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

public final class ScyllaStatementHandler {
    
    private static final ConcurrentMap<Set<String>, PreparedStatement> INSERT_STMTS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<List<Comparison>, PreparedStatement> FIND_ONE_STMTS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Set<String>, PreparedStatement> UPDATE_ONE_STMTS = new ConcurrentHashMap<>();

    static int bindPreparedFineOneStatement(BoundStatement stmt, String table, List<Comparison> filters, Set<String> fields) {
        if(fields != null) throw new IllegalArgumentException("reading specific fields is not supported yet.");
        return FilterBuilder.bindFilter(0, stmt, filters);
    }
    static PreparedStatement getOrBuildPreparedFindOneStatement(String table, List<Comparison> filters, Set<String> fields) {
        if(fields != null) throw new IllegalArgumentException("reading specific fields is not supported yet.");
        // fields is null, so it does not have to be considered; will need to be changed at some later stage
        final List<Comparison> normalizedFilters = new ArrayList<>();
        for(Comparison c : filters) {
            normalizedFilters.add(c.normalized());
        }
        PreparedStatement stmt = FIND_ONE_STMTS.get(normalizedFilters);
        if(stmt != null) return stmt;
        // does not exist, create one
        Select s = QueryBuilder.select().all()
            .from(ScyllaCQLClient.keyspace, table)
            .limit(1).allowFiltering();
        FilterBuilder.buildPreparedFilter(normalizedFilters, s.where());
        synchronized(FIND_ONE_STMTS) {
            // someone may have added the element here, try again
            stmt = FIND_ONE_STMTS.get(normalizedFilters);
            if(stmt != null) return stmt;
            stmt = ScyllaCQLClient.session.prepare(s);
            stmt.setConsistencyLevel(ScyllaCQLClient.writeConsistencyLevel);
            stmt.setIdempotent(true);
            if (ScyllaCQLClient.trace) {
                stmt.enableTracing();
            }
            PreparedStatement prevStmt = FIND_ONE_STMTS.putIfAbsent(normalizedFilters, stmt);
            if (prevStmt != null) {
                stmt = prevStmt;
            }
        }
        return stmt;
    }
    static void bindPreparedUpdateOneStatment(String primKey, int startIndex, List<DatabaseField> fields, BoundStatement bound) {
        int nextIndex = FilterBuilder.bindPreparedAssignments(bound, fields, startIndex);
        bound.setString(nextIndex, primKey);
    }
    static PreparedStatement getOrBuildPreparedUpdateOneStatement(String table, List<DatabaseField> fields) {
        final Set<String> normalizedFields = new HashSet<>();
        for(DatabaseField f : fields) {
            if(!f.getContent().isTerminal()) {
                throw new IllegalArgumentException("nested elements are not supported for scylla");
            }
            normalizedFields.add(f.getFieldname());
        }
        PreparedStatement stmt = UPDATE_ONE_STMTS.get(normalizedFields);
        if(stmt != null) return stmt;
        // does not exist, create one
        Update u = QueryBuilder.update(ScyllaCQLClient.keyspace, table);
        FilterBuilder.buildPreparedAssignments(fields, u.with());
        u.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));
        if(ScyllaCQLClient.debug) {
            u.enableTracing();
        }
        synchronized(UPDATE_ONE_STMTS) {
            // someone may have added the element here, try again
            stmt = UPDATE_ONE_STMTS.get(normalizedFields);
            if(stmt != null) return stmt;
            stmt = ScyllaCQLClient.session.prepare(u);
            stmt.setConsistencyLevel(ScyllaCQLClient.writeConsistencyLevel);
            stmt.setIdempotent(false);
            if (ScyllaCQLClient.trace) {
                stmt.enableTracing();
            }
            PreparedStatement prevStmt = UPDATE_ONE_STMTS.putIfAbsent(normalizedFields, stmt);
            if (prevStmt != null) {
                stmt = prevStmt;
            }
        }
        return stmt;
    }

    static PreparedStatement getOrBuildPreparedInsertStatement(Set<String> fields, String table) {
        PreparedStatement stmt = INSERT_STMTS.get(fields);
        if(stmt != null) return stmt;
        // does not exist, create one
        Insert insertStmt = QueryBuilder.insertInto(table);
      
        // Add key
        insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());

        // Add fields
        for (String field : fields) {
            insertStmt.value(field, QueryBuilder.bindMarker());
        }

        if (ScyllaCQLClient.lwt) {
            insertStmt.ifNotExists();
        }
        // we cannot risk running this part concurrently, so let's lock it
        // insert statements are the same anyway, so this will only lock once
        synchronized(INSERT_STMTS) {
            // someone may have added the element here, try again
            stmt = INSERT_STMTS.get(fields);
            if(stmt != null) return stmt;
            stmt = ScyllaCQLClient.session.prepare(insertStmt);
            stmt.setConsistencyLevel(ScyllaCQLClient.writeConsistencyLevel);
            if (ScyllaCQLClient.trace) {
                stmt.enableTracing();
            }
            PreparedStatement prevStmt = INSERT_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
            if (prevStmt != null) {
                stmt = prevStmt;
            }
        }
        return stmt;
    }

    static void cleanup() {
        INSERT_STMTS.clear();
        FIND_ONE_STMTS.clear();
    }
    private ScyllaStatementHandler() {
        // never called
    }
}
