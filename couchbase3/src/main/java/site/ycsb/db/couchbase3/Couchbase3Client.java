/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
 * Copyright (c) 2023-204 benchANT GmbH. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agrlaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.couchbase3;

import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveBucket;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.couchbase.client.java.query.ReactiveQueryResult;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

/**
 * A class that wraps the 3.x Couchbase SDK to be used with YCSB.
 *
 * <p> The following options can be passed when using this database client to override the defaults.
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=ycsb</b> The bucket name to use.</li>
 * <li><b>couchbase.scope=_default</b> The scope to use.</li>
 * <li><b>couchbase.collection=_default</b> The collection to use.</li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.durability=</b> Durability level to use.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement.</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement.</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.adhoc=false</b> If set to true, prepared statements are not used.</li>
 * <li><b>couchbase.maxParallelism=1</b> The server parallelism for all n1ql queries.</li>
 * <li><b>couchbase.kvEndpoints=1</b> The number of KV sockets to open per server.</li>
 * <li><b>couchbase.sslMode=false</b> Set to true to use SSL to connect to the cluster.</li>
 * <li><b>couchbase.sslNoVerify=true</b> Set to false to check the SSL server certificate.</li>
 * <li><b>couchbase.certificateFile=</b> Path to file containing certificates to trust.</li>
 * </ul>
 */

public class Couchbase3Client extends DB implements IndexableDB {
  private static final String MAX_RETRY_PROPERTY = "couchbase.maxretry";
  private static final String MAX_RETRY_DEFAULT = "0";
  public static final String INDEX_LIST_PROPERTY = "couchbase.indexlist";
  // private static final Logger LOGGER = LoggerFactory.getLogger(Couchbase3Client.class.getName());
  private static final String KEY_SEPARATOR = ":";
  private static final String KEYSPACE_SEPARATOR = ".";
  private static volatile ClusterEnvironment environment;
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  static volatile Cluster cluster;
  private static volatile ReactiveCluster reactiveCluster;
  private static volatile Bucket bucket;
  private static volatile ClusterOptions clusterOptions;
  private volatile DurabilityLevel durabilityLevel;
  private volatile PersistTo persistTo;
  private volatile ReplicateTo replicateTo;
  private volatile boolean useDurabilityLevels;
  private volatile ArrayList<Throwable> errors = new ArrayList<>();
  private boolean adhoc;
  private int maxParallelism;
  static String bucketName;
  private String scopeName;
  private String collectionName;
  private static boolean collectionEnabled;
  private static boolean scopeEnabled;
  private static String username;
  private static String password;
  private static String hostname;
  private static long kvTimeoutMillis;
  private static long queryTimeoutMillis;
  private static int kvEndpoints;
  private boolean upsert;
  private static boolean sslMode;
  private static boolean sslNoVerify;
  private String certificateFile;
  private static String keyspaceName;
  private static volatile AtomicInteger primaryKeySeq;
  private static int numRetries;
  private static boolean useTypedFields;
  /** The batch size to use for inserts. */
  private static int batchSize;
  /** The bulk inserts pending for the thread. */
  private final Map<String, Map<String,Object>> bulkInserts = new HashMap<String, Map<String,Object>>();
  private static boolean debug = false;
  // private java.util.Base64.Encoder encoder = java.util.Base64.getEncoder();
  private static final int MAX_ERRORS = 1024;

  private void addToErrors(Throwable t) {
    if(errors.size() < MAX_ERRORS) {
      errors.add(t);
    }
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    primaryKeySeq = new AtomicInteger();
    debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
    bucketName = props.getProperty("couchbase.bucket", "ycsb");
    scopeName = props.getProperty("couchbase.scope", "_default");
    collectionName = props.getProperty("couchbase.collection", "_default");
    scopeEnabled = scopeName != "_default";
    collectionEnabled = collectionName != "_default";
    keyspaceName = getKeyspaceName();
    useTypedFields = "true".equalsIgnoreCase(props.getProperty(TYPED_FIELDS_PROPERTY));
    // Set insert batchsize, default 1 - to be YCSB-original equivalent
    batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
    numRetries = Integer.parseInt(props.getProperty(MAX_RETRY_PROPERTY, MAX_RETRY_DEFAULT));
    String rawDurabilityLevel = props.getProperty("couchbase.durability", null);
    if (rawDurabilityLevel != null) {
      if (props.containsKey("couchbase.persistTo") || props.containsKey("couchbase.replicateTo")) {
        throw new DBException("Durability setting and persist/replicate settings are mutually exclusive.");
      }
      try {
        durabilityLevel = parseDurabilityLevel(rawDurabilityLevel);
        useDurabilityLevels = true;
      } catch (DBException e) {
        System.err.println("Failed to parse durability level using defaults");
      }
    } else {
      try {
        persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
        replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
        useDurabilityLevels = false;
      } catch (DBException e) {
        System.err.println("Failed to parse persist/replicate levels using defaults");
      }
    }

    adhoc = props.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty("couchbase.maxParallelism", "0"));
    upsert = props.getProperty("couchbase.upsert", "false").equals("true");

    hostname = props.getProperty("couchbase.host", "127.0.0.1");
    username = props.getProperty("couchbase.username", "Administrator");
    password = props.getProperty("couchbase.password", "password");

    sslMode = props.getProperty("couchbase.sslMode", "false").equals("true");
    sslNoVerify = props.getProperty("couchbase.sslNoVerify", "true").equals("true");
    certificateFile = props.getProperty("couchbase.certificateFile", "none");

    synchronized (INIT_COORDINATOR) {
      if (environment == null) {

        boolean enableMutationToken = Boolean.parseBoolean(props.getProperty("couchbase.enableMutationToken", "false"));

        kvTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.kvTimeout", "2000"));
        queryTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.queryTimeout", "14000"));
        kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));

        if (sslMode) {
          ClusterEnvironment.Builder clusterEnvironment = ClusterEnvironment
              .builder()
                .timeoutConfig(
                TimeoutConfig.builder()
                .kvTimeout(Duration.ofMillis(kvTimeoutMillis))
                .queryTimeout(Duration.ofMillis(queryTimeoutMillis))
              )
              .ioConfig(IoConfig.builder()
                .enableMutationTokens(enableMutationToken)
                .numKvConnections(kvEndpoints)
              );

          if (sslNoVerify) {
            clusterEnvironment.securityConfig(SecurityConfig.enableTls(true)
                .enableHostnameVerification(false)
                .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
          } else if (!certificateFile.equals("none")) {
            clusterEnvironment.securityConfig(SecurityConfig.enableTls(true)
                .trustCertificate(Paths.get(certificateFile)));
          } else {
            clusterEnvironment.securityConfig(SecurityConfig.enableTls(true));
          }

          environment = clusterEnvironment.build();
        } else {
          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(
                TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
              .ioConfig(IoConfig.enableMutationTokens(enableMutationToken)
                  .numKvConnections(kvEndpoints))
              .build();
        }

        clusterOptions = ClusterOptions.clusterOptions(username, password);
        clusterOptions.environment(environment);
        cluster = Cluster.connect(hostname, clusterOptions);
        reactiveCluster = cluster.reactive();
        bucket = cluster.bucket(bucketName);

        List<JsonObject> indexes = Couchbase3IndexHelper.getIndexList(props);
        Couchbase3IndexHelper.setIndexes(props, indexes);
      }
    }
    OPEN_CLIENTS.incrementAndGet();
  }

  /**
   * Checks the replicate parameter value.
   * @param property provided replicateTo parameter.
   * @return ReplicateTo value.
   */
  private static ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  /**
   * Checks the persist parameter value.
   * @param property provided persistTo parameter.
   * @return PersistTo value.
   */
  private static PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  /**
   * Checks the durability parameter.
   * @param property provided durability parameter.
   * @return DurabilityLevel value.
   */
  private static DurabilityLevel parseDurabilityLevel(final String property) throws DBException {

    int value = Integer.parseInt(property);

    switch(value){
    case 0:
      return DurabilityLevel.NONE;
    case 1:
      return DurabilityLevel.MAJORITY;
    case 2:
      return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
    case 3:
      return DurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
    }
  }

  @Override
  public synchronized void cleanup() {
    int clients = OPEN_CLIENTS.decrementAndGet();
    if (clients == 0 && environment != null) {
      cluster.disconnect();
      environment.shutdown();
      environment = null;
    }
    System.err.println(Thread.currentThread().getName() + ": dumping errors");
    Iterator<Throwable> it = errors.iterator();
      while(it.hasNext()) {
        Throwable t = (Throwable)it.next();
        t.printStackTrace(System.err);
      }
  }

    /**
   * Helper function to generate the keyspace name.
   * @return a string with the computed keyspace name
   */
  private String getKeyspaceName() {
    if (scopeEnabled || collectionEnabled) {
      return bucketName + KEYSPACE_SEPARATOR + this.scopeName + KEYSPACE_SEPARATOR + this.collectionName;
    } else {
      return bucketName;
    }
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key the key itself.
   * @return a document ID that can be used with Couchbase.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }

  private Status batchInsert(final String table, final String key, Map<String,Object> encoding) {
    final String theId = formatId(table, key);
    bulkInserts.put(theId, encoding);
    if(bulkInserts.size() < batchSize) {
      return Status.BATCHED_OK;
    }
    // System.exit(-1);
    final Map<String, Map<String,Object>> localInserts = new HashMap<String, Map<String,Object>>(bulkInserts);
    bulkInserts.clear();
    ReactiveBucket rBucket = bucket.reactive();
    ReactiveCollection collection = collectionEnabled
      ? rBucket.scope(this.scopeName).collection(this.collectionName)
      : rBucket.defaultCollection();
    // bulk inserts has right size, let's send it
    // Iterate over a list of documents to insert.
    CommonDurabilityOptions<?> opts = upsert
      ? upsertOptions()
      : insertOptions();
    if (useDurabilityLevels) {
      opts.durability(durabilityLevel);
    } else {
      opts.durability(persistTo, replicateTo);
    }
    while (true) {
      int retryCount = 0;
      final Map<String, Map<String,Object>> failed = new HashMap<String, Map<String,Object>>();
      List<MutationResult> results = Flux.fromIterable(localInserts.keySet())
      .<MutationResult>flatMap(id -> {
        if(upsert) {
          return collection
            .upsert(id, localInserts.get(id), (UpsertOptions) opts )
            .onErrorResume(t -> {
              addToErrors(t);
              if(debug) {
                System.err.println("one insert failed in batch upsert loop with exception");
                t.printStackTrace(System.err);
              }
              failed.put(id, encoding); return Mono.empty();
            });
        }
        else{
          return collection
            .insert(id, localInserts.get(id), (InsertOptions) opts )
            .onErrorResume(t -> {
              addToErrors(t);
              if(debug) {
                System.err.println("one insert failed in batch insert loop with exception :");
                t.printStackTrace(System.err);
              }
              failed.put(id, encoding); return Mono.empty();
            });
        }
      }).collectList()
      .block(); // Wait until all operations have completed. 
      if(failed.isEmpty()) {
        return Status.OK;
      }
      // at least one document failed
      if (retryCount == numRetries) {
          return Status.ERROR;
      } else {
        Couchbase3QueryHelper.retryWait(retryCount);
      }
      retryCount++;
      localInserts.clear();;
      localInserts.putAll(failed);
    }
  }
  /**
   * Insert a record.
   * @param table The name of the table.
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record.
   */
  @Override
  public Status insert(final String table, final String key, List<DatabaseField> values_) {
    int retryCount = 0;
    Map<String,?> encoding = useTypedFields
      ? Couchbase3QueryHelper.encodeWithTypes(values_)
      : Couchbase3QueryHelper.encode(DB.fieldListAsIteratorMap(values_));
    if(batchSize > 1) {
      return batchInsert(table, key, (Map<String,Object> )encoding);
    }
    while (true) {
      try {
        Collection collection = collectionEnabled ?
            bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
        // not adding "record_id" as other implementations will need to runs scans as well
        // values.put("record_id", new StringByteIterator(String.valueOf(primaryKeySeq.incrementAndGet())));
        if (useDurabilityLevels) {
          if (upsert) {
            collection.upsert(formatId(table, key), encoding, upsertOptions().durability(durabilityLevel));
          } else {
            collection.insert(formatId(table, key), encoding, insertOptions().durability(durabilityLevel));
          }
        } else {
          if (upsert) {
            collection.upsert(formatId(table, key), encoding, upsertOptions().durability(persistTo, replicateTo));
          } else {
            collection.insert(formatId(table, key), encoding, insertOptions().durability(persistTo, replicateTo));
          }
        }
        return Status.OK;
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("insert failed with exception :");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          Couchbase3QueryHelper.retryWait(retryCount);
          retryCount++;
        }
      }
    }
  }

  /**
   * Perform key/value read ("get").
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A HashMap of field/value pairs for the result.
   */
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    int retryCount = 0;
    while (true) {
      try {
        Collection collection = collectionEnabled ?
            bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
        GetResult document = collection.get(formatId(table, key));
        Couchbase3QueryHelper.extractFields(document.contentAsObject(), fields, result);
        return Status.OK;
      } catch (DocumentNotFoundException e) {
        return Status.NOT_FOUND;
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("read failed with exception : ");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          retryCount++;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
      }
    }
  }

  /**
   * Query for specific rows of data using SQL++.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    int retryCount = 0;
    while (true) {
      try {
        if (fields == null || fields.isEmpty()) {
          return scanAllFields(table, startkey, recordcount, result);
        } else {
          return scanSpecificFields(table, startkey, recordcount, fields, result);
        }
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("scan failed with exception");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          ++retryCount;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
      }
    }
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation for all fields.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                               final Vector<HashMap<String, ByteIterator>> result) {

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query = "SELECT record_id FROM " + keyspaceName +
        " WHERE record_id >= \"$1\" ORDER BY record_id LIMIT $2";
    QueryOptions scanQueryOptions = QueryOptions.queryOptions();

    if (maxParallelism > 0) {
      scanQueryOptions.maxParallelism(maxParallelism);
    }

    cluster.reactive().query(query,
            scanQueryOptions
            .pipelineBatch(128)
            .pipelineCap(1024)
            .scanCap(1024)
            .adhoc(adhoc)
            .readonly(true)
            .parameters(JsonArray.from(numericId(startkey), recordcount)))
            .flatMapMany(ReactiveQueryResult::rowsAsObject)
              .onErrorResume(e -> {
                  if(debug) {
                    System.err.println("Start Key: " + startkey + " Count: "
                      + recordcount + " Error:" + e.getClass() + " Info: " + e.getMessage());
                  }
                  return Mono.empty();
                })
              .map(row -> {
                  HashMap<String, ByteIterator> tuple = new HashMap<>();
                  tuple.put("record_id", new StringByteIterator(row.getString("record_id")));
                  return tuple;
                })
              .toStream()
              .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }
  /**
   * Helper function to convert the key to a numeric value.
   * @param key the key text
   * @return a string with non-numeric characters removed
   */
  private static String numericId(final String key) {
    return key.replaceAll("[^\\d.]", "");
  }
  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation only for a subset of the fields.
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */

  private Status scanSpecificFields(final String table, final String startkey, final int recordcount,
                                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    final Collection collection = bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query =  "SELECT RAW meta().id FROM " + keyspaceName +
        " WHERE record_id >= $1 ORDER BY record_id LIMIT $2";
    final ReactiveCollection reactiveCollection = collection.reactive();
    QueryOptions scanQueryOptions = QueryOptions.queryOptions();

    if (maxParallelism > 0) {
      scanQueryOptions.maxParallelism(maxParallelism);
    }

    reactiveCluster.query(query,
            scanQueryOptions
            .adhoc(adhoc)
            .parameters(JsonArray.from(numericId(startkey), recordcount)))
        .flatMapMany(res -> {
            return res.rowsAs(String.class);
          })
        .flatMap(id -> {
            return reactiveCollection
              .get(id, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
          })
        .map(getResult -> {
            HashMap<String, ByteIterator> tuple = new HashMap<>();
            decodeStringSource(getResult.contentAs(String.class), fields, tuple);
            return tuple;
          })
        .toStream()
        .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }
  /**
   * Get string values from fields.
   * @param source JSON source data.
   * @param fields Fields to return.
   * @param dest Map of Strings where each value is a requested field.
   */
  private void decodeStringSource(final String source, final Set<String> fields,
                      final Map<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.asText()));
        }
      }
    } catch (Exception e) {
      if(debug) {
          System.err.println("Could not decode JSON response from scanSpecificFields");
      }
    }
  }
  /**
   * Update record.
   * @param table The name of the table.
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record.
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    int retryCount = 0;
    while (true) {
      try {
        Collection collection = collectionEnabled ?
            bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
        values.put("record_id", new StringByteIterator(String.valueOf(primaryKeySeq.incrementAndGet())));
        if (useDurabilityLevels) {
          collection.replace(formatId(table, key),
            Couchbase3QueryHelper.encode(values), replaceOptions().durability(durabilityLevel));
        } else {
          collection.replace(formatId(table, key),
            Couchbase3QueryHelper.encode(values), replaceOptions().durability(persistTo, replicateTo));
        }
        return Status.OK;
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("update failed with exception");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          retryCount++;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
      }
    }
  }

  /**
   * Remove a record.
   * @param table The name of the table.
   * @param key The record key of the record to delete.
   */
  @Override
  public Status delete(final String table, final String key) {
    int retryCount = 0;
    while (true) {
      try {
        Collection collection = collectionEnabled ?
            bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
        if (useDurabilityLevels) {
          collection.remove(formatId(table, key), removeOptions().durability(durabilityLevel));
        } else {
          collection.remove(formatId(table, key), removeOptions().durability(persistTo, replicateTo));
        }
        return Status.OK;
      } catch (DocumentNotFoundException dnf) {
        return Status.NOT_FOUND;
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("delete failed with exception");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          retryCount++;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
      }
    }
  }
  @Override
  public Status findOne(String table, List<Comparison> filters, Set<String> fields,
    Map<String, ByteIterator> result) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    if(fields != null) {
      throw new UnsupportedOperationException("cannot read results by field");
    }
    
    String query = Couchbase3QueryBuilder.buildFindOnePlaceholderQuery(keyspaceName, filters);
    JsonArray params = JsonArray.create();
    Couchbase3QueryBuilder.bindFindOneQuery(params, filters);
    if(debug) {
      System.err.println("sending query and params:\n\t" + query + "\n\t" + params.toString());
    }
    int retryCount = 0;
    while (true) {
      try {
        QueryOptions options = QueryOptions.queryOptions();
        QueryResult qResult = cluster.query(query, options
                      .adhoc(adhoc)
                      .readonly(true)
                      .parameters(params)
                      .maxParallelism(maxParallelism)
                      // .metrics(true)
        );
        if(qResult.metaData().status() != QueryStatus.SUCCESS) {
          if(debug) {
            System.err.println("unexpected query status: " + qResult.metaData().status());
          }
          return Status.UNEXPECTED_STATE;
        }
        // no other way found to get the amount of results
        List<JsonObject> returned = qResult.rowsAsObject();
        if(returned.size() == 0) {
          return Status.NOT_FOUND;
        }
        if(returned.size() > 1) {
          return Status.UNEXPECTED_STATE;
        }
        Couchbase3QueryHelper.extractTypedFields(returned.get(0), fields, result);
        return Status.OK;
      } catch (DocumentNotFoundException dnf) {
        return Status.NOT_FOUND;
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("updateOne failed with exception");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          retryCount++;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
      }
    }
  }
  @Override
  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    if(fields == null || fields.size() == 0) {
      throw new NullPointerException();
    }
    String query = Couchbase3QueryBuilder.buildUpdateOnePlaceholderQuery(keyspaceName, filters, fields);
    JsonArray params = JsonArray.create();
    Couchbase3QueryBuilder.bindUpdateOneQuery(params, fields, filters);
    if(debug) {
      System.err.println("sending query and params:\n\t" + query + "\n\t" + params.toString());
    }
    int retryCount = 0;
    while (true) {
      try {
        QueryOptions options = QueryOptions.queryOptions();
        // QueryResult qResult = bucket.defaultScope().query(query, 
        QueryResult qResult = cluster.query(query, 
          options.adhoc(adhoc)
                      .parameters(params)
                      .readonly(false)
                      .maxParallelism(maxParallelism)
                      // .asTransaction(SingleQueryTransactionOptions.singleQueryTransactionOptions().durabilityLevel(DurabilityLevel.NONE))
                      // .metrics(true)
        );
        if(qResult.metaData().status() != QueryStatus.SUCCESS) {
          if(debug) {
            System.err.println("unexpected query status: " + qResult.metaData().status());
          }
          return Status.UNEXPECTED_STATE;
        }
        List<JsonObject> returned = qResult.rowsAsObject();
        // System.err.println("returnd: " + returned.size());
        if(returned.size() == 0) {
          return Status.NOT_FOUND;
        }
        if(returned.size() > 1) {
          return Status.UNEXPECTED_STATE;
        }
        // extractTypedFields(returned.get(0), fields, result);
        return Status.OK;
      } catch (DocumentNotFoundException dnf) {
        return Status.NOT_FOUND;
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("updateOne failed with exception");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          retryCount++;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
      }
    }
  }
}