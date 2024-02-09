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

import com.datastax.driver.core.ConsistencyLevel;

public final class ScyllaDbConstants {
    
    public static final String INIT_TABLE = "scylla.inittable";
    public static final String INDEX_LIST_PROPERTY = "scylla.indexlist";
    public static final String YCSB_KEY = "y_id";
    public static final String KEYSPACE_PROPERTY = "scylla.keyspace";
    public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
    public static final String USERNAME_PROPERTY = "scylla.username";
    public static final String PASSWORD_PROPERTY = "scylla.password";
  
    public static final String HOSTS_PROPERTY = "scylla.hosts";
    public static final String PORT_PROPERTY = "scylla.port";
    public static final String PORT_PROPERTY_DEFAULT = "9042";
  
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "scylla.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = ConsistencyLevel.QUORUM.name();
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "scylla.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = ConsistencyLevel.QUORUM.name();
  
    public static final String MAX_CONNECTIONS_PROPERTY = "scylla.maxconnections";
    public static final String CORE_CONNECTIONS_PROPERTY = "scylla.coreconnections";
    public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "scylla.connecttimeoutmillis";
    public static final String READ_TIMEOUT_MILLIS_PROPERTY = "scylla.readtimeoutmillis";
  
    public static final String REPLICATION_STRATEGY_CLASS_PROPERTY = "scylla.replicationclass";
    public static final String REPLICATION_STRATEGY_CLASS_DEFAULT = "org.apache.cassandra.locator.SimpleStrategy";

    public static final String REPLICATION_DEGREE_PROPERTY = "scylla.replicationdegree";
    public static final String REPLICATION_DEGREE_DEFAULT = "1";

    public static final String SCYLLA_LWT = "scylla.lwt";
  
    public static final String TOKEN_AWARE_LOCAL_DC = "scylla.local_dc";
  
    public static final String TRACING_PROPERTY = "scylla.tracing";
    public static final String TRACING_PROPERTY_DEFAULT = "false";
  
    public static final String USE_SSL_CONNECTION = "scylla.useSSL";
    public static final String DEFAULT_USE_SSL_CONNECTION = "false";

    private ScyllaDbConstants() {
        // empty
    }
}
