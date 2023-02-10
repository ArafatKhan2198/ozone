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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.hadoop.ozone.recon.schema.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;

/**
 * Class to iterate over the OM DB and store the counts of existing/new
 * files binned into ranges (1KB, 2Kb..,4MB,.., 1TB,..1PB) to the Recon
 * fileSize DB.
 */
public class FileSizeCountTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSizeCountTask.class);

  private FileCountBySizeDao fileCountBySizeDao;
  private DSLContext dslContext;

  @Inject
  public FileSizeCountTask(FileCountBySizeDao fileCountBySizeDao,
                           UtilizationSchemaDefinition
                               utilizationSchemaDefinition) {
    this.fileCountBySizeDao = fileCountBySizeDao;
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
  }

  /**
   * Read the Keys from OM snapshot DB and calculate the upper bound of
   * File Size it belongs to.
   *
   * @param omMetadataManager OM Metadata instance.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    Table<String, OmKeyInfo> omKeyInfoTable =
        omMetadataManager.getKeyTable(getBucketLayout());
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        keyIter = omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        handlePutKeyEvent(kv.getValue(), fileSizeCountMap);
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count in Recon DB. ", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    // Truncate table before inserting new rows
    int execute = dslContext.delete(FILE_COUNT_BY_SIZE).execute();
    LOG.info("Deleted {} records from {}", execute, FILE_COUNT_BY_SIZE);

    writeCountsToDB(true, fileSizeCountMap);

    LOG.info("Completed a 'reprocess' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public String getTaskName() {
    return "FileSizeCountTask";
  }

  public Collection<String> getTaskTables() {
    return Collections.singletonList(KEY_TABLE);
  }

  /**
   * Read the Keys from update events and update the count of files
   * pertaining to a certain upper bound.
   *
   * @param events Update events - PUT/DELETE.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    final Collection<String> taskTables = getTaskTables();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ?> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();

      // Get the updated and old OM Key Info objects
      Object omKeyInfo = omdbUpdateEvent.getValue();
      Object oldOmKeyInfo = omdbUpdateEvent.getOldValue();

      OmKeyInfo keyInfo, oldKeyInfo;
      // Handle the case where the updated OM Key Info is a RepeatedOmKeyInfo object
      if (omKeyInfo instanceof RepeatedOmKeyInfo) {
        // Handle RepeatedOmKeyInfo object
        RepeatedOmKeyInfo repeatedKeyInfo = (RepeatedOmKeyInfo) omKeyInfo;
        keyInfo = repeatedKeyInfo.getOmKeyInfoList().get(0);
        oldKeyInfo = repeatedKeyInfo.getOmKeyInfoList().get(0);
      }
      // Handle the case where the updated OM Key Info is an OmKeyInfo object
      else {
        // Handle OmKeyInfo object
        keyInfo = (OmKeyInfo) omKeyInfo;
        oldKeyInfo = (OmKeyInfo) oldOmKeyInfo;
      }

      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutKeyEvent(keyInfo, fileSizeCountMap);
          break;

        case DELETE:
          handleDeleteKeyEvent(updatedKey, keyInfo, fileSizeCountMap);
          break;

        case UPDATE:
          handleDeleteKeyEvent(updatedKey, oldKeyInfo,
              fileSizeCountMap);
          handlePutKeyEvent(keyInfo, fileSizeCountMap);
          break;

        default: LOG.trace("Skipping DB update event : {}",
            omdbUpdateEvent.getAction());
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception while processing key {}.",
                updatedKey, e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    writeCountsToDB(false, fileSizeCountMap);
    LOG.info("Completed a 'process' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * Populate DB with the counts of file sizes calculated
   * using the dao.
   *
   */
  private void writeCountsToDB(boolean isDbTruncated,
                               Map<FileSizeCountKey, Long> fileSizeCountMap) {
    List<FileCountBySize> insertToDb = new ArrayList<>();
    List<FileCountBySize> updateInDb = new ArrayList<>();

    fileSizeCountMap.keySet().forEach((FileSizeCountKey key) -> {
      FileCountBySize newRecord = new FileCountBySize();
      newRecord.setVolume(key.volume);
      newRecord.setBucket(key.bucket);
      newRecord.setFileSize(key.fileSizeUpperBound);
      newRecord.setCount(fileSizeCountMap.get(key));
      if (!isDbTruncated) {
        // Get the current count from database and update
        Record3<String, String, Long> recordToFind =
            dslContext.newRecord(
                FILE_COUNT_BY_SIZE.VOLUME,
                FILE_COUNT_BY_SIZE.BUCKET,
                FILE_COUNT_BY_SIZE.FILE_SIZE)
                .value1(key.volume)
                .value2(key.bucket)
                .value3(key.fileSizeUpperBound);
        FileCountBySize fileCountRecord =
            fileCountBySizeDao.findById(recordToFind);
        if (fileCountRecord == null && newRecord.getCount() > 0L) {
          // insert new row only for non-zero counts.
          insertToDb.add(newRecord);
        } else if (fileCountRecord != null) {
          newRecord.setCount(fileCountRecord.getCount() +
              fileSizeCountMap.get(key));
          updateInDb.add(newRecord);
        }
      } else if (newRecord.getCount() > 0) {
        // insert new row only for non-zero counts.
        insertToDb.add(newRecord);
      }
    });
    fileCountBySizeDao.insert(insertToDb);
    fileCountBySizeDao.update(updateInDb);
  }

  private FileSizeCountKey getFileSizeCountKey(OmKeyInfo omKeyInfo) {
    return new FileSizeCountKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(),
            ReconUtils.getFileSizeUpperBound(omKeyInfo.getDataSize()));
  }

  /**
   * Calculate and update the count of files being tracked by
   * fileSizeCountMap.
   * Used by reprocess() and process().
   *
   * @param omKeyInfo OmKey being updated for count
   */
  private void handlePutKeyEvent(OmKeyInfo omKeyInfo,
                                 Map<FileSizeCountKey, Long> fileSizeCountMap) {
    FileSizeCountKey key = getFileSizeCountKey(omKeyInfo);
    Long count = fileSizeCountMap.containsKey(key) ?
        fileSizeCountMap.get(key) + 1L : 1L;
    fileSizeCountMap.put(key, count);
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  /**
   * Calculate and update the count of files being tracked by
   * fileSizeCountMap.
   * Used by reprocess() and process().
   *
   * @param omKeyInfo OmKey being updated for count
   */
  private void handleDeleteKeyEvent(String key, OmKeyInfo omKeyInfo,
                                    Map<FileSizeCountKey, Long>
                                        fileSizeCountMap) {
    if (omKeyInfo == null) {
      LOG.warn("Deleting a key not found while handling DELETE key event. Key" +
          " not found in Recon OM DB : {}", key);
    } else {
      FileSizeCountKey countKey = getFileSizeCountKey(omKeyInfo);
      Long count = fileSizeCountMap.containsKey(countKey) ?
          fileSizeCountMap.get(countKey) - 1L : -1L;
      fileSizeCountMap.put(countKey, count);
    }
  }

  private static class FileSizeCountKey {
    private String volume;
    private String bucket;
    private Long fileSizeUpperBound;

    FileSizeCountKey(String volume, String bucket,
                     Long fileSizeUpperBound) {
      this.volume = volume;
      this.bucket = bucket;
      this.fileSizeUpperBound = fileSizeUpperBound;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FileSizeCountKey) {
        FileSizeCountKey s = (FileSizeCountKey) obj;
        return volume.equals(s.volume) && bucket.equals(s.bucket) &&
            fileSizeUpperBound.equals(s.fileSizeUpperBound);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (volume  + bucket + fileSizeUpperBound).hashCode();
    }
  }
}
