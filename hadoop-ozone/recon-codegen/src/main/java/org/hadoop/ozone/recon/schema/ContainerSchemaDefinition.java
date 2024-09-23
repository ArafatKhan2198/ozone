/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hadoop.ozone.recon.schema;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Class used to create tables that are required for tracking containers.
 */
@Singleton
public class ContainerSchemaDefinition implements ReconSchemaDefinition {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerSchemaDefinition.class);

  public static final String UNHEALTHY_CONTAINERS_TABLE_NAME =
      "UNHEALTHY_CONTAINERS";

  /**
   * ENUM describing the allowed container states which can be stored in the
   * unhealthy containers table.
   */
  public enum UnHealthyContainerStates {
    MISSING,
    EMPTY_MISSING,
    UNDER_REPLICATED,
    OVER_REPLICATED,
    MIS_REPLICATED,
    ALL_REPLICAS_BAD,
    NEGATIVE_SIZE // Added new state to track containers with negative sizes
  }

  private static final String CONTAINER_ID = "container_id";
  private static final String CONTAINER_STATE = "container_state";
  private final DataSource dataSource;
  private DSLContext dslContext;

  @Inject
  ContainerSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      dslContext = DSL.using(conn);
      if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        createUnhealthyContainersTable();
      }
    } catch (SQLException e) {
      LOG.error("Error initializing schema", e);
      throw e;
    }
  }

  @Override
  public void upgradeSchema(String fromVersion, String toVersion) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        return;
      }
      if (fromVersion.equals("0") && toVersion.equals("1.0")) {
        runMigrationToVersion1(conn);
        LOG.info("Upgraded schema from version 0 to 1.0.");
      }
      // Add more upgrade paths here as needed
    } catch (SQLException e) {
      LOG.error("Error upgrading schema", e);
      throw e;
    }
  }


  /**
   * Run the upgrade to version 1.0.
   */
  private void runMigrationToVersion1(Connection conn) throws SQLException {
    // Drop the existing constraint if it exists
    String constraintName = UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1";
    dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .dropConstraint(constraintName)
        .execute();

    // Get all enum values as a list of strings
    String[] enumStates = Arrays.stream(UnHealthyContainerStates.values())
        .map(Enum::name)
        .toArray(String[]::new);

    // Alter the table to add the updated constraint
    dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .add(DSL.constraint(UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1")
            .check(field(name("container_state"))
                .in(enumStates)))
        .execute();
  }

  /**
   * Create the Missing Containers table.
   */
  private void createUnhealthyContainersTable() {
    dslContext.createTableIfNotExists(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .column(CONTAINER_ID, SQLDataType.BIGINT.nullable(false))
        .column(CONTAINER_STATE, SQLDataType.VARCHAR(16).nullable(false))
        .column("in_state_since", SQLDataType.BIGINT.nullable(false))
        .column("expected_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("actual_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("replica_delta", SQLDataType.INTEGER.nullable(false))
        .column("reason", SQLDataType.VARCHAR(500).nullable(true))
        .constraint(DSL.constraint("pk_container_id")
            .primaryKey(CONTAINER_ID, CONTAINER_STATE))
        .constraint(DSL.constraint(UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1")
            .check(field(name("container_state"))
                .in(UnHealthyContainerStates.values())))
        .execute();
  }

  public DSLContext getDSLContext() {
    return dslContext;
  }
}
