/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016, 2019 YCSB contributors. All rights reserved.
 * Copyright 2023-2024 benchANT GmbH. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import site.ycsb.db.flavors.DBFlavor;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

import static site.ycsb.db.JdbcDBConstants.*;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 *
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 *
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type TEXT. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class JdbcDBClient extends DB implements IndexableDB {
  static class IndexDescriptor {
    String name;
    String method;
    String order;
    boolean concurrent;
    List<String> columnNames = new ArrayList<>();
  }

  static enum TableInitStatus {
    ERROR,
    COMPLETE,
    VOID,
  }
  private static volatile TableInitStatus tableInitStatus = TableInitStatus.VOID;
  /** SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY. */
  private boolean sqlansiScans = false;
  /** SQL Server before 2012: TOP n after the SELECT. */
  private boolean sqlserverScans = false;

  private List<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;
  private static boolean useTypedFields;
  static boolean debug = false;
  /**
   * Ordered field information for insert and update statements.
   * only used for untyped version of this driver
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    int timeout = Integer.parseInt(props.getProperty(CONNECTION_TIMEOUT_PROPERTY, CONNECTION_TIMEOUT_DEFAULT));
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);

    try {
//  The SQL Syntax for Scan depends on the DB engine
//  - SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY
//  - SQL Server before 2012: TOP n after the SELECT
//  - others (MySQL,MariaDB, PostgreSQL before 8.4)
//  TODO: check product name and version rather than driver name
      if (driver != null) {
        if (driver.contains("sqlserver")) {
          sqlserverScans = true;
          sqlansiScans = false;
        }
        if (driver.contains("oracle")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        if (driver.contains("postgres")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      // for a longer explanation see the README.md
      // semicolons aren't present in JDBC urls, so we use them to delimit
      // multiple JDBC connections to shard across.
      final String[] urlArr = urls.split(";");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        if(timeout > -1) {
          System.out.println("setting timeout to: " + timeout);
          DriverManager.setLoginTimeout(timeout);
        }
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.fromJdbcUrl(urlArr[0]);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }
    boolean initDb = "true".equalsIgnoreCase(props.getProperty(JDBC_INIT_TABLE, "false"));
    useTypedFields = "true".equalsIgnoreCase(props.getProperty(TYPED_FIELDS_PROPERTY));
    List<IndexDescriptor> indexes = JdbcDBInitHelper.getIndexList(getProperties());
    final String table = props.getProperty(CoreConstants.TABLENAME_PROPERTY, CoreConstants.TABLENAME_PROPERTY_DEFAULT);
    synchronized(JdbcDBClient.class) {
      debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
      if(tableInitStatus == TableInitStatus.ERROR) {
        throw new IllegalStateException("table initialization or index creation failed, terminating");
      }
      if(tableInitStatus == TableInitStatus.VOID) {
        // it is our task to init the table
        try {
          if(initDb) {
            JdbcDBInitHelper.createDbAndSchema(table, conns);
          }
          // build indexes only once, but once per connection
          List<String> indexCommands = JdbcDBInitHelper.buildIndexCommands(table, indexes);
          for(Connection c : conns) {
            for(String cmd : indexCommands) {
              Statement stmt = c.createStatement();
              int ret = stmt.executeUpdate(cmd);
              System.out.println("created index: " + cmd + ": " + "/" + ret);
            }
          }
          tableInitStatus = TableInitStatus.COMPLETE;
        } catch(SQLException ex) {
          tableInitStatus = TableInitStatus.ERROR;
          throw new IllegalStateException("could not create table or indexes, terminating", ex);
        }
      }
    }
    initialized = true;
  }


  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
      throws SQLException {
    String insert = dbFlavor.createInsertStatement(insertType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert);
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
      throws SQLException {
    String read = dbFlavor.createReadStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) {
      return readStatement;
    }
    return stmt;
  }

  private final ConcurrentHashMap<UpdateContainer, PreparedStatement> UPDATE_ONE_STATEMENTS = new ConcurrentHashMap<>();
  static final class UpdateContainer {
    final List<Comparison> filters;
    final Set<String> field;
    public UpdateContainer(List<Comparison> filters, Set<String> field) {
      this.filters = filters;
      this.field = field;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((filters == null) ? 0 : filters.hashCode());
      result = prime * result + ((field == null) ? 0 : field.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      UpdateContainer other = (UpdateContainer) obj;
      if (filters == null) {
        if (other.filters != null)
          return false;
      } else if (!filters.equals(other.filters))
        return false;
      if (field == null) {
        if (other.field != null)
          return false;
      } else if (!field.equals(other.field))
        return false;
      return true;
    }
    
  }
  private PreparedStatement createAndCacheUpdateOneStatement(
      String tablename, List<Comparison> filters, List<DatabaseField> fields)
      throws SQLException {
    final List<Comparison> normalizedFilters = new ArrayList<>();
    for(Comparison c : filters) {
      normalizedFilters.add(c.normalized());
    }
    final Set<String> updates = new HashSet<>();
    for(DatabaseField f: fields) {
      if(f.getContent().isTerminal()) {
        updates.add(f.getFieldname());
      } else {
        throw new IllegalStateException("non-terminals not supported");
      }
    }
    UpdateContainer container = new UpdateContainer(normalizedFilters, updates);
    PreparedStatement ret = UPDATE_ONE_STATEMENTS.get(container);
    if(ret != null) return ret;
    if(debug){
      System.err.println("updateOne query not found: creating it");
    }
    String query = JdbcQueryHelper.createUpdateOneStatement(tablename, filters, fields);
    synchronized(UPDATE_ONE_STATEMENTS) {
      ret = UPDATE_ONE_STATEMENTS.get(container);
      if(ret != null) return ret;
      PreparedStatement prepStatement = conns.get(0).prepareStatement(query);
      ret = UPDATE_ONE_STATEMENTS.putIfAbsent(container, prepStatement);
      if (ret == null) {
        ret = prepStatement;
      }
    }
    return ret;
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) {
      return deleteStatement;
    }
    return stmt;
  }

  private final ConcurrentHashMap<List<Comparison>, PreparedStatement> FIND_ONE_STATEMENTS = new ConcurrentHashMap<>();
  private PreparedStatement createAndCacheFindOneStatement(
      String tablename, List<Comparison> filters, Set<String> fields)
      throws SQLException {
    if(fields != null) {
      throw new IllegalStateException("reading specific fields currently not supported by this driver");
    }
    final List<Comparison> normalizedFilters = new ArrayList<>();
    for(Comparison c : filters) {
      normalizedFilters.add(c.normalized());
    }
    PreparedStatement ret = FIND_ONE_STATEMENTS.get(normalizedFilters);
    if(ret != null) return ret;
    if(debug){
      System.err.println("findOne query not found: creating it");
    }
    String query = JdbcQueryHelper.createFindOneStatement(tablename, filters, fields);
    synchronized(FIND_ONE_STATEMENTS) {
      ret = FIND_ONE_STATEMENTS.get(normalizedFilters);
      if(ret != null) return ret;
      PreparedStatement prepStatement = conns.get(0).prepareStatement(query);
      ret = FIND_ONE_STATEMENTS.putIfAbsent(normalizedFilters, prepStatement);
      if (ret == null) {
        ret = prepStatement;
      }
    }
    return ret;
  }

  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateStatement(updateType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
      throws SQLException {
    String select = dbFlavor.createScanStatement(scanType, key, sqlserverScans, sqlansiScans);
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select);
    if (this.jdbcFetchSize > 0) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) {
      return scanStatement;
    }
    return stmt;
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, "", getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      // SQL Server TOP syntax is at first
      if (sqlserverScans) {
        scanStatement.setInt(1, recordcount);
        scanStatement.setString(2, startKey);
      // FETCH FIRST and LIMIT are at the end
      } else {
        scanStatement.setString(1, startKey);
        scanStatement.setInt(2, recordcount);
      }
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
      int index = 1;
      for (String value: fieldInfo.getFieldValues()) {
        updateStatement.setString(index++, value);
      }
      updateStatement.setString(index, key);
      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }
  private void buildTypedQuery(PreparedStatement insertStatement, List<DatabaseField> values) throws SQLException {
    int index = 1;
    for (DatabaseField value: values) {
      // increase index count immediately
      // as index 1 has already been taken
      index++;
      DataWrapper w = value.getContent();
      if(w.isTerminal()) {
        if(w.isInteger()) {
          insertStatement.setInt(index, w.asInteger());
        } else if(w.isLong()) {
          insertStatement.setLong(index, w.asLong());
        } else if(w.isString()) {
          insertStatement.setString(index, w.asString());
        } else {
          // assuming this is an iterator
          // which is the only remaining terminal
          Object o = w.asObject();
          byte[] b = (byte[]) o;
          insertStatement.setString(index, new String(b));
        }
      } else if(w.isArray()) {
        throw new UnsupportedOperationException("array type columns not supported (yet)");
      } else if(w.isNested()) {
        throw new UnsupportedOperationException("nested type columns not supported (yet)");
      } else {
        // do nothing; something broke that we ignore for now
      }
    }
  }
  private void buildLegacyQuery(PreparedStatement insertStatement, List<DatabaseField> values) throws SQLException {
    int index = 2;
    for (DatabaseField value: values) {
      String v = value.getContent().asIterator().toString();
      insertStatement.setString(index++, v);
    }
  }
  @Override
  public Status insert(String tableName, String key, List<DatabaseField> values) {
    try {
      int numFields = values.size();
      String fieldNameString = getFieldNameString(values);
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName,
          numFields, fieldNameString, getShardIndexByKey(key));
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type, key);
      }
      insertStatement.setString(1, key);
      
      if(useTypedFields) {
        buildTypedQuery(insertStatement, values);
      } else {
        buildLegacyQuery(insertStatement, values);
      }
      // Using the batch insert API
      if (batchUpdates) {
        insertStatement.addBatch();
        // Check for a sane batch size
        if (batchSize > 0) {
          // Commit the batch after it grows beyond the configured size
          if (++numRowsInBatch % batchSize == 0) {
            int[] results = insertStatement.executeBatch();
            for (int r : results) {
              // Acceptable values are 1 and SUCCESS_NO_INFO (-2) from reWriteBatchedInserts=true
              if (r != 1 && r != -2) { 
                return Status.ERROR;
              }
            }
            // If autoCommit is off, make sure we commit the batch
            if (!autoCommit) {
              getShardConnectionByKey(key).commit();
            }
            return Status.OK;
          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large batch.
        } // else, we let the batch accumulate
        // Added element to the batch, potentially committing the batch too.
        return Status.BATCHED_OK;
      } else {
        // Normal update
        int result = insertStatement.executeUpdate();
        // If we are not autoCommit, we might have to commit now
        if (!autoCommit) {
          // Let updates be batcher locally
          if (batchSize > 0) {
            if (++numRowsInBatch % batchSize == 0) {
              // Send the batch of updates
              getShardConnectionByKey(key).commit();
            }
            // uhh
            return Status.OK;
          } else {
            // Commit each update
            getShardConnectionByKey(key).commit();
          }
        }
        if (result == 1) {
          return Status.OK;
        }
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      int result = deleteStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      if(result == 0) {
        return Status.NOT_FOUND;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }
    private String getFieldNameString(List<DatabaseField> values) {
      List<String> els = new ArrayList<>();
      values.forEach(v -> els.add(v.getFieldname()));
      return String.join(",", els);
    }

  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList<>();
    int count = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      fieldValues.add(count, entry.getValue().toString());
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }
@Override
  public Status findOne(String tableName, List<Comparison> filters, Set<String> fields,
      Map<String, ByteIterator> result) {
    if(!useTypedFields) {
      throw new IllegalStateException("only works with typed fields");
    }
    if(fields != null) {
      throw new IllegalStateException("reading specific fields currently not supported by this driver");
    }
    try {
      PreparedStatement statement = createAndCacheFindOneStatement(tableName, filters, fields);
      JdbcQueryHelper.bindFindOneStatement(statement, filters, fields);
      ResultSet resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      FilterBuilder.drainTypedResult(resultSet, result);
      if(resultSet.next()) {
        System.err.println("Error in processing find of table " + tableName + ": too many results");
        resultSet.close();
        return Status.ERROR;
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing find on table: " + tableName + e);
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }
  @Override
  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    if(!useTypedFields) {
      throw new IllegalStateException("only works with typed fields");
    }
    try {
      PreparedStatement statement = createAndCacheUpdateOneStatement(table, filters, fields);
      JdbcQueryHelper.bindUpdateOneStatement(statement, filters, fields);
      int resultSet = statement.executeUpdate();
      if (resultSet == 0) {
        return Status.NOT_FOUND;
      }
      if (resultSet > 1) {
        return Status.UNEXPECTED_STATE;
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing update on table: " + table + e);
      return Status.ERROR;
    }
  }
}
