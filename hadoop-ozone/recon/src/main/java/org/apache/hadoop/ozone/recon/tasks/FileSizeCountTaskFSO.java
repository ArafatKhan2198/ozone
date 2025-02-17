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

package org.apache.hadoop.ozone.recon.tasks;

import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.hadoop.ozone.recon.schema.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;

/**
 * Task for FileSystemOptimized (FSO) which processes the FILE_TABLE.
 */
public class FileSizeCountTaskFSO implements ReconOmTask {
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(FileSizeCountTaskFSO.class);

  private final FileCountBySizeDao fileCountBySizeDao;
  private final DSLContext dslContext;

  @Inject
  public FileSizeCountTaskFSO(FileCountBySizeDao fileCountBySizeDao,
                              UtilizationSchemaDefinition utilizationSchemaDefinition) {
    this.fileCountBySizeDao = fileCountBySizeDao;
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    return FileSizeCountTaskHelper.reprocess(
        omMetadataManager,
        dslContext,
        fileCountBySizeDao,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        getTaskName()
    );
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    // This task listens only on the FILE_TABLE.
    return FileSizeCountTaskHelper.processEvents(
        events,
        OmMetadataManagerImpl.FILE_TABLE,
        dslContext,
        fileCountBySizeDao,
        getTaskName());
  }

  @Override
  public String getTaskName() {
    return "FileSizeCountTaskFSO";
  }
}
