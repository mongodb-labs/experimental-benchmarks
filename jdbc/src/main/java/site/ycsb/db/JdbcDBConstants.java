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

public final class JdbcDBConstants {
    
    public static final String INDEX_LIST_PROPERTY = "jdbc.indexlist";
    /** The class to use as the jdbc driver. */
    public static final String DRIVER_CLASS = "db.driver";

    public static final String CONNECTION_TIMEOUT_PROPERTY = "db.connectionTimeout";
    public static final String CONNECTION_TIMEOUT_DEFAULT = "-1";

    /** The URL to connect to the database. */
    public static final String CONNECTION_URL = "db.url";
  
    /** The user name to use to connect to the database. */
    public static final String CONNECTION_USER = "db.user";
  
    /** The password to use for establishing the connection. */
    public static final String CONNECTION_PASSWD = "db.passwd";
  
    /** The batch size for batched inserts. Set to >0 to use batching */
    public static final String DB_BATCH_SIZE = "db.batchsize";
  
    /** The JDBC fetch size hinted to the driver. */
    public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";
  
    /** The JDBC fetch size hinted to the driver. */
    public static final String JDBC_INIT_TABLE = "jdbc.inittable";
  
    /** The JDBC connection auto-commit property for the driver. */
    public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";
  
    public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";
    
    /** The primary key in the user table. */
    public static final String PRIMARY_KEY = "YCSB_KEY";

    private JdbcDBConstants() {
        // no instances
    }
}
