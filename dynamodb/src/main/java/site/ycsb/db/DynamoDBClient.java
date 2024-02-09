/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexUpdate;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.Status;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

/**
 * DynamoDB client for YCSB.
 */

public class DynamoDBClient extends DB implements IndexableDB {
  static class IndexDescriptor {
    String name;
    String hashKeyAttribute;
    int readCap;
    int writeCap;
    List<String> sortsKeyAttributes = new ArrayList<>();
    @Override
    public String toString() {
      return "IndexDescriptor [name=" + name + ", hashKeyAttribute=" + hashKeyAttribute + ", readCap=" + readCap
          + ", writeCap=" + writeCap + ", sortsKeyAttributes=" + sortsKeyAttributes + "]";
    }
  }
  /**
   * Defines the primary key type used in this particular DB instance.
   * <p>
   * By default, the primary key type is "HASH". Optionally, the user can
   * choose to use hash_and_range key type. See documentation in the
   * DynamoDB.Properties file for more details.
   */
  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }
  public static final String INDEX_LIST_PROPERTY = "dynamodb.indexlist";
  public static final String INDEX_READ_CAP_PROPERTY = "dynamodb.indexreadcap";
  public static final String INDEX_READ_CAP_DEFAULT = "5";
  public static final String INDEX_WRITE_CAP_PROPERTY = "dynamodb.indexwritecap";
  public static final String INDEX_WRITE_CAP_DEFAULT = "5";
  private AmazonDynamoDB dynamoDB;
  String primaryKeyName;
  PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private String hashKeyValue;
  private String hashKeyName;

  private boolean consistentRead = false;
  private String region = "us-east-1";
  private String endpoint = null;
  private int maxConnects = 50;
  private int maxRetries = -1;
  static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";
  private static boolean useTypedFields;
  /** The batch size to use for inserts. */
  private static int batchSize;
  private static final int MAX_RETRY = 3;
  private static int defaultReadCap;
  private static int defaultWriteCap;
  private static List<IndexDescriptor> indexes = null;
  /** The bulk inserts pending for the thread. */
  private final List<WriteRequest> bulkInserts = new ArrayList<WriteRequest>();

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("dynamodb.debug", null);

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    batchSize = Integer.parseInt(getProperties().getProperty("batchsize", "1"));
    String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
    String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile", null);
    String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
    String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);
    String connectMax = getProperties().getProperty("dynamodb.connectMax", null);
    maxRetries = Integer.parseInt(getProperties().getProperty("dynamodb.maxRetries", "0"));
    String configuredRegion = getProperties().getProperty("dynamodb.region", null);

    if (null != connectMax) {
      this.maxConnects = Integer.parseInt(connectMax);
    }

    if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
      this.consistentRead = true;
    }

    if (null != configuredEndpoint) {
      this.endpoint = configuredEndpoint;
    }

    if (null == primaryKey || primaryKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }

    if (null != primaryKeyTypeString) {
      try {
        this.primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid primary key mode specified: " + primaryKeyTypeString +
            ". Expecting HASH or HASH_AND_RANGE.");
      }
    }
    useTypedFields = "true".equalsIgnoreCase(getProperties().getProperty(TYPED_FIELDS_PROPERTY));
    if (this.primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // When the primary key type is HASH_AND_RANGE, keys used by YCSB
      // are range keys so we can benchmark performance of individual hash
      // partitions. In this case, the user must specify the hash key's name
      // and optionally can designate a value for the hash key.

      String configuredHashKeyName = getProperties().getProperty("dynamodb.hashKeyName", null);
      if (null == configuredHashKeyName || configuredHashKeyName.isEmpty()) {
        throw new DBException("Must specify a non-empty hash key name when the primary key type is HASH_AND_RANGE.");
      }
      this.hashKeyName = configuredHashKeyName;
      this.hashKeyValue = getProperties().getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
    }

    if (null != configuredRegion && configuredRegion.length() > 0) {
      region = configuredRegion;
    }
    if(batchSize > 25) {
      throw new DBException("batch site must be <= 25 for dynamodb.");
    }
    try {
      AmazonDynamoDBClientBuilder dynamoDBBuilder = AmazonDynamoDBClientBuilder.standard();
      dynamoDBBuilder = null == endpoint ?
          dynamoDBBuilder.withRegion(this.region) :
          dynamoDBBuilder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(this.endpoint, this.region)
          );
      dynamoDBBuilder
          .withClientConfiguration(
              new ClientConfiguration()
                  .withTcpKeepAlive(true)
                  .withMaxConnections(this.maxConnects)
                  .withMaxErrorRetry(maxRetries)
          );
      if(credentialsFile != null) {
        dynamoDBBuilder.withCredentials(new AWSStaticCredentialsProvider(new PropertiesCredentials(new File(credentialsFile))));
      } else {
        dynamoDBBuilder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        LOGGER.info("using credentials from environment variables");
      }
      dynamoDB = dynamoDBBuilder.build();
      primaryKeyName = primaryKey;
      LOGGER.info("dynamodb connection created with " + this.endpoint);
    } catch (Exception e1) {
      LOGGER.error("DynamoDBClient.init(): Could not initialize DynamoDB client.", e1);
    }
    synchronized(DynamoDBClient.class) {
      if(indexes == null) {
        final String table = getProperties().getProperty(CoreConstants.TABLENAME_PROPERTY, CoreConstants.TABLENAME_PROPERTY_DEFAULT);
        defaultReadCap = Integer.parseInt(getProperties().getProperty(INDEX_READ_CAP_PROPERTY, INDEX_READ_CAP_DEFAULT));
        defaultWriteCap = Integer.parseInt(getProperties().getProperty(INDEX_WRITE_CAP_PROPERTY, INDEX_WRITE_CAP_DEFAULT));
        List<IndexDescriptor> localIndexes = DynamoDBInitHelper.getIndexList(getProperties(), defaultReadCap, defaultWriteCap);
        LOGGER.info("indexes loaded from config: " + localIndexes.toString());
        setIndexes(table, getProperties(), localIndexes);
        indexes = loadRemoteIndexes(table);
        LOGGER.info("loaded remote idexes: " + indexes.toString());
      }
    }
  }

  private List<IndexDescriptor> loadRemoteIndexes(String table) {
      DescribeTableResult tableDesc = dynamoDB.describeTable(table);
      List<GlobalSecondaryIndexDescription> indexes = tableDesc.getTable().getGlobalSecondaryIndexes();
      if(indexes == null) {
        return Collections.emptyList();
      }
      List<IndexDescriptor> result = new ArrayList<>(indexes.size());
      for(GlobalSecondaryIndexDescription index : indexes) {
        IndexDescriptor desc = new IndexDescriptor();
        desc.name = index.getIndexName();
        List<KeySchemaElement> schema = index.getKeySchema();
        for(KeySchemaElement el : schema) {
          if(KeyType.HASH.toString().equals(el.getKeyType())) {
            desc.hashKeyAttribute = el.getAttributeName();
          } else if(KeyType.RANGE.toString().equals(el.getKeyType())) {
            desc.sortsKeyAttributes.add(el.getAttributeName());
          }
        }
        result.add(desc);
      }
      return result;
  }

  private void waitForIndexCreation(String table, String name) {
    LOGGER.info("initial wait");
    try {
      Thread.sleep(10000);
    } catch(InterruptedException ex) {
      // ignore for now
    }
    while(true) {
      DescribeTableResult result = dynamoDB.describeTable(table);
      List<GlobalSecondaryIndexDescription> indexes = result.getTable().getGlobalSecondaryIndexes();
      GlobalSecondaryIndexDescription myIndex = null;
      String names = "";
      for(GlobalSecondaryIndexDescription index : indexes) {
        names = names + index.getIndexName() + ", ";
        if(name.equals(index.getIndexName())) {
          myIndex = index;
        }
      }
      if(myIndex == null) {
        throw new IllegalStateException("Newly created index " + name + " was not in index list: " + names);
      }
      final String status = myIndex.getIndexStatus();
      LOGGER.info("status of newly created index " + name + " " + status);
      if(IndexStatus.CREATING.toString().equals(status) ||
          IndexStatus.UPDATING.toString().equals(status)) {
        LOGGER.info("waiting more");
        try {
          Thread.sleep(10000);
        } catch(InterruptedException ex) {
          // ignore for now
        }
      } else {
        LOGGER.info("let's move on");
        break;
      }
    }
  }

  private void setIndexes(String table, Properties props, List<IndexDescriptor> indexes) {
    // we do not return here, as it is beneficial to set table properties
    final boolean setProperties = props.getProperty("dynamodb.settableproperties", "false").equals("true");
    if(indexes.size() == 0 && !setProperties) {
      return;
    }
    List<AttributeDefinition> attributes = DynamoDBInitHelper.getFullAttributeDefinitionList();
    attributes.add(new AttributeDefinition().withAttributeName(primaryKeyName).withAttributeType(ScalarAttributeType.S));
    if(indexes.size() == 0 && setProperties) {
      UpdateTableRequest req = new UpdateTableRequest()
        .withAttributeDefinitions(attributes)
        .withTableName(table);
        UpdateTableResult result = dynamoDB.updateTable(req);
        System.err.println("updated table");
    }
    for(IndexDescriptor idx : indexes) {
      CreateGlobalSecondaryIndexAction action = DynamoDBInitHelper.getCreateSecondaryIndexAction(idx);
      GlobalSecondaryIndexUpdate up = new GlobalSecondaryIndexUpdate().withCreate(action);
      UpdateTableRequest req = new UpdateTableRequest()
        .withGlobalSecondaryIndexUpdates(up)
        .withAttributeDefinitions(attributes)
        .withTableName(table);
      UpdateTableResult result = dynamoDB.updateTable(req);
      waitForIndexCreation(table, idx.name);
      System.err.println("prepared index creation: " + idx);
    }
    System.err.println("all indexes created");
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: " + key + " from table: " + table);
    }

    GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
    req.setAttributesToGet(fields);
    req.setConsistentRead(consistentRead);
    GetItemResult res;

    try {
      res = dynamoDB.getItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != res.getItem()) {
      result.putAll(DynamoDBQueryParameterHelper.extractResult(res.getItem()));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result: " + res.toString());
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    }
    /*
     * on DynamoDB's scan, startkey is *exclusive* so we need to
     * getItem(startKey) and then use scan for the res
    */
    GetItemRequest greq = new GetItemRequest(table, createPrimaryKey(startkey));
    greq.setAttributesToGet(fields);

    GetItemResult gres;
    try {
      gres = dynamoDB.getItem(greq);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    if (null != gres.getItem()) {
      result.add(DynamoDBQueryParameterHelper.extractResult(gres.getItem()));
    }

    int count = 1; // startKey is done, rest to go.
    Map<String, AttributeValue> startKey = createPrimaryKey(startkey);
    ScanRequest req = new ScanRequest(table);
    req.setAttributesToGet(fields);
    while (count < recordcount) {
      req.setExclusiveStartKey(startKey);
      req.setLimit(recordcount - count);
      ScanResult res;
      try {
        res = dynamoDB.scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.getCount();
      for (Map<String, AttributeValue> items : res.getItems()) {
        result.add(DynamoDBQueryParameterHelper.extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    Map<String, AttributeValueUpdate> attributes = new HashMap<>(values.size());
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      AttributeValue v = new AttributeValue(val.getValue().toString());
      attributes.put(val.getKey(), new AttributeValueUpdate().withValue(v).withAction("PUT"));
    }
    
    return internalUpdateItem(table, key, attributes);
  }

  @Override
  public Status insert(String table, String key, List<DatabaseField> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
    }

    Map<String, AttributeValue> attributes = useTypedFields
      ? DynamoDBQueryParameterHelper.createTypedAttributes(values)
      : DynamoDBQueryParameterHelper.createAttributes(fieldListAsIteratorMap(values));
    // adding primary key
    attributes.put(primaryKeyName, new AttributeValue(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, new AttributeValue(hashKeyValue));
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: sending attributes " + attributes);
    }
    try {
      if(batchSize < 2) {
        PutItemRequest putItemRequest = new PutItemRequest(table, attributes);
        putItemRequest.setConditionExpression("attribute_not_exists(" + primaryKeyName + ")");
        dynamoDB.putItem(putItemRequest);
      } else {
        WriteRequest ww = new WriteRequest(new PutRequest(attributes));
        bulkInserts.add(ww);
        if(bulkInserts.size() < batchSize) { return Status.BATCHED_OK; }
        List<WriteRequest> local = new ArrayList<>(bulkInserts);
        bulkInserts.clear();
        for(int retries = 0; retries < MAX_RETRY && local.size() > 0; retries++) {
          BatchWriteItemRequest batch = new BatchWriteItemRequest();
          batch.addRequestItemsEntry(table, local);
          BatchWriteItemResult result = dynamoDB.batchWriteItem(batch);
          local = result.getUnprocessedItems().get(table);
          if(local == null || local.size() == 0) { return Status.OK; }
          LOGGER.error("could not process all requests in a batch: " + local.size() + " requests missing; retrying.");
        }
        return Status.ERROR;
      }
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    } 
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }
    DeleteItemRequest req = new DeleteItemRequest(table, createPrimaryKey(key));
    try {
      dynamoDB.deleteItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  private IndexDescriptor findIndexMatchingFilter(List<Comparison> filters) {
    for(Comparison c : filters) {
      for(IndexDescriptor idx : indexes) {
        if(c.getFieldname().equals(idx.hashKeyAttribute)) {
          return idx;
        }
        // FIXME: to be really correct, the filter does have to 
        // check for equality as well
      }
    }
    return null;
  }

  public Status internalFindOne(String table, List<Comparison> filters,
        Set<String> fields,List<Item> resultList) {
      IndexDescriptor desc = findIndexMatchingFilter(filters);
      if(desc == null) {
        LOGGER.error("did not find a matching index for filter: " + filters);
        return Status.ERROR;
      } else {
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("found matching index for filters: " + filters + " => " + desc);
        }
      }
      Table dbTable = new DynamoDB(dynamoDB).getTable(table);
      Index idx = dbTable.getIndex(desc.name);
      QuerySpec query = new QuerySpec().withConsistentRead(consistentRead);
      DynamoDBQueryHelper.buildPreparedQuery(query, desc.hashKeyAttribute, filters);
      DynamoDBQueryHelper.bindPreparedQuery(query, filters);
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("query spec: " + query.getRequest());
      }
      IteratorSupport<Item, QueryOutcome> col = idx.query(query).iterator();
      if(!col.hasNext()) {
        return Status.NOT_FOUND;
      }
      Item item = col.next();
      if(col.hasNext()) {
        LOGGER.error("retrieved more than one element");
        return Status.ERROR;
      }
      resultList.add(item);
      return Status.OK;
  }

  @Override
  public Status findOne(String table, List<Comparison> filters, Set<String> fields,
    Map<String, ByteIterator> result) {
      if(fields != null) {
        throw new IllegalArgumentException("fields can only be null by now");
      }
      List<Item> resultList = new ArrayList<>(1);
      Status proxy = internalFindOne(table, filters, fields, resultList);
      if(proxy != Status.OK) return proxy;
      result.putAll(DynamoDBQueryParameterHelper.extractResultFromItem(resultList.get(0)));
      return Status.OK;
  }

  private Status internalUpdateItem(String table, String localPrimaryKey, Map<String, AttributeValueUpdate> attributes){
    Map<String, AttributeValue> primaryKey = createPrimaryKey(localPrimaryKey);
    UpdateItemRequest req = new UpdateItemRequest(table, primaryKey, attributes);

    try {
      dynamoDB.updateItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    List<Item> resultList = new ArrayList<>(1);
    Status found = internalFindOne(table, filters, null, resultList);
    if(found != Status.OK) return found;
    Item it = resultList.get(0);
    String id = it.getString(primaryKeyName);
    Map<String, AttributeValueUpdate> attributes = new HashMap<>();
    Map<String, AttributeValue> plainAttributes = DynamoDBQueryParameterHelper.createTypedAttributes(fields);
    for(Map.Entry<String, AttributeValue> e : plainAttributes.entrySet()) {
      attributes.put(e.getKey(), new AttributeValueUpdate().withValue(e.getValue()).withAction("PUT"));
    }
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("updating item: " + id + " with " + plainAttributes);
    }
    return internalUpdateItem(table, id, attributes);
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else {
      throw new RuntimeException("Assertion Error: impossible primary key type");
    }
    return k;
  }
}