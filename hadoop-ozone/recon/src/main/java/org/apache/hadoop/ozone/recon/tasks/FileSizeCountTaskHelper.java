/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.HashMap;

/**
 * Helper class that encapsulates the common code for file size count tasks.
 * This class does not implement {@code ReconOmTask}; instead, it exposes static
 * helper methods that require all needed objects (e.g. DSLContext, DAO, OMMetadataManager)
 * to be passed as arguments.
 */
public abstract class FileSizeCountTaskHelper {
  protected static final Logger LOG = LoggerFactory.getLogger(FileSizeCountTaskHelper.class);

  /**
   * Truncates the FILE_COUNT_BY_SIZE table if it has not been truncated yet.
   *
   * @param dslContext DSLContext for executing DB commands.
   */
  public static void truncateTableIfNeeded(DSLContext dslContext) {
    if (ReconConstants.FILE_SIZE_COUNT_TABLE_TRUNCATED.compareAndSet(false, true)) {
      int execute = dslContext.delete(FILE_COUNT_BY_SIZE).execute();
      LOG.info("Deleted {} records from {}", execute, FILE_COUNT_BY_SIZE);
    } else {
      LOG.info("Table already truncated by another task; skipping deletion.");
    }
  }

  /**
   * Executes the reprocess method for the given task.
   *
   * @param omMetadataManager  OM metadata manager.
   * @param dslContext         DSLContext for DB operations.
   * @param fileCountBySizeDao DAO for file count table.
   * @param bucketLayout       The bucket layout to process.
   * @param taskName           The name of the task (for logging).
   * @return A Pair of task name and boolean indicating success.
   */
  public static Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager,
                                                DSLContext dslContext,
                                                FileCountBySizeDao fileCountBySizeDao,
                                                BucketLayout bucketLayout,
                                                String taskName) {
    LOG.info("Starting Reprocess for {}", taskName);
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    long startTime = System.currentTimeMillis();
    truncateTableIfNeeded(dslContext);
    boolean status = reprocessBucketLayout(
        bucketLayout, omMetadataManager, fileSizeCountMap, dslContext, fileCountBySizeDao, taskName);
    if (!status) {
      return new ImmutablePair<>(taskName, false);
    }
    writeCountsToDB(fileSizeCountMap, dslContext, fileCountBySizeDao);
    long endTime = System.currentTimeMillis();
    LOG.info("{} completed Reprocess in {} ms.", taskName, (endTime - startTime));
    return new ImmutablePair<>(taskName, true);
  }

  /**
   * Iterates over the OM DB keys for the given bucket layout and updates the fileSizeCountMap.
   *
   * @param bucketLayout       The bucket layout to use.
   * @param omMetadataManager  OM metadata manager.
   * @param fileSizeCountMap   Map accumulating file size counts.
   * @param dslContext         DSLContext for DB operations.
   * @param fileCountBySizeDao DAO for file count table.
   * @param taskName           The name of the task (for logging).
   * @return true if processing succeeds, false otherwise.
   */
  public static boolean reprocessBucketLayout(BucketLayout bucketLayout,
                                              OMMetadataManager omMetadataManager,
                                              Map<FileSizeCountKey, Long> fileSizeCountMap,
                                              DSLContext dslContext,
                                              FileCountBySizeDao fileCountBySizeDao,
                                              String taskName) {
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(bucketLayout);
    int totalKeysProcessed = 0;
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter =
             omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        handlePutKeyEvent(kv.getValue(), fileSizeCountMap);
        totalKeysProcessed++;

        // Flush to DB periodically.
        if (fileSizeCountMap.size() >= 100000) {
          writeCountsToDB(fileSizeCountMap, dslContext, fileCountBySizeDao);
          fileSizeCountMap.clear();
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count for {} in Recon DB.", taskName, ioEx);
      return false;
    }
    LOG.info("Reprocessed {} keys for bucket layout {}.", totalKeysProcessed, bucketLayout);
    return true;
  }

  /**
   * Processes a batch of OM update events.
   *
   * @param events             OM update event batch.
   * @param taskTables         The tables this task listens to.
   * @param dslContext         DSLContext for DB operations.
   * @param fileCountBySizeDao DAO for file count table.
   * @param taskName           The name of the task (for logging).
   * @return A Pair of task name and boolean indicating success.
   */
  public static Pair<String, Boolean> processEvents(OMUpdateEventBatch events,
                                                    String bucketLayout,
                                                    DSLContext dslContext,
                                                    FileCountBySizeDao fileCountBySizeDao,
                                                    String taskName) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();
    long startTime = System.currentTimeMillis();
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();
      if (!bucketLayout.equals(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();
      Object value = omdbUpdateEvent.getValue();
      Object oldValue = omdbUpdateEvent.getOldValue();
      if (value instanceof OmKeyInfo) {
        OmKeyInfo omKeyInfo = (OmKeyInfo) value;
        OmKeyInfo omKeyInfoOld = (OmKeyInfo) oldValue;
        try {
          switch (omdbUpdateEvent.getAction()) {
          case PUT:
            handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
            break;
          case DELETE:
            handleDeleteKeyEvent(updatedKey, omKeyInfo, fileSizeCountMap);
            break;
          case UPDATE:
            if (omKeyInfoOld != null) {
              handleDeleteKeyEvent(updatedKey, omKeyInfoOld, fileSizeCountMap);
              handlePutKeyEvent(omKeyInfo, fileSizeCountMap);
            } else {
              LOG.warn("Update event does not have the old keyInfo for {}.", updatedKey);
            }
            break;
          default:
            LOG.trace("Skipping DB update event: {}", omdbUpdateEvent.getAction());
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception while processing key {}.", updatedKey, e);
          return new ImmutablePair<>(taskName, false);
        }
      } else {
        LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
            value.getClass().getName(), updatedKey);
      }
    }
    writeCountsToDB(fileSizeCountMap, dslContext, fileCountBySizeDao);
    LOG.debug("{} successfully processed in {} milliseconds", taskName,
        (System.currentTimeMillis() - startTime));
    return new ImmutablePair<>(taskName, true);
  }

  /**
   * Writes the accumulated file size counts to the DB.
   *
   * @param fileSizeCountMap   Map of file size counts.
   * @param dslContext         DSLContext for DB operations.
   * @param fileCountBySizeDao DAO for file count table.
   */
  public static void writeCountsToDB(Map<FileSizeCountKey, Long> fileSizeCountMap,
                                     DSLContext dslContext,
                                     FileCountBySizeDao fileCountBySizeDao) {

    List<FileCountBySize> insertToDb = new ArrayList<>();
    List<FileCountBySize> updateInDb = new ArrayList<>();
    boolean isDbTruncated = isFileCountBySizeTableEmpty(dslContext); // Check if table is empty

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

  /**
   * Increments the count for a given key (on a PUT event).
   */
  public static void handlePutKeyEvent(OmKeyInfo omKeyInfo,
                                       Map<FileSizeCountKey, Long> fileSizeCountMap) {
    FileSizeCountKey key = getFileSizeCountKey(omKeyInfo);
    Long count = fileSizeCountMap.containsKey(key) ? fileSizeCountMap.get(key) + 1L : 1L;
    fileSizeCountMap.put(key, count);
  }

  /**
   * Decrements the count for a given key (on a DELETE event).
   */
  public static void handleDeleteKeyEvent(String key, OmKeyInfo omKeyInfo,
                                          Map<FileSizeCountKey, Long> fileSizeCountMap) {
    if (omKeyInfo == null) {
      LOG.warn("Deleting a key not found while handling DELETE key event. Key not found in Recon OM DB: {}", key);
    } else {
      FileSizeCountKey countKey = getFileSizeCountKey(omKeyInfo);
      Long count = fileSizeCountMap.containsKey(countKey) ? fileSizeCountMap.get(countKey) - 1L : -1L;
      fileSizeCountMap.put(countKey, count);
    }
  }

  /**
   * Returns a FileSizeCountKey for the given OmKeyInfo.
   */
  public static FileSizeCountKey getFileSizeCountKey(OmKeyInfo omKeyInfo) {
    return new FileSizeCountKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(),
        ReconUtils.getFileSizeUpperBound(omKeyInfo.getDataSize()));
  }

  /**
   * Checks if the FILE_COUNT_BY_SIZE table is empty.
   */
  public static boolean isFileCountBySizeTableEmpty(DSLContext dslContext) {
    return dslContext.fetchCount(FILE_COUNT_BY_SIZE) == 0;
  }

  /**
   * Helper key class used for grouping file size counts.
   */
  public static class FileSizeCountKey {
    private final String volume;
    private final String bucket;
    private final Long fileSizeUpperBound;

    public FileSizeCountKey(String volume, String bucket, Long fileSizeUpperBound) {
      this.volume = volume;
      this.bucket = bucket;
      this.fileSizeUpperBound = fileSizeUpperBound;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FileSizeCountKey) {
        FileSizeCountKey other = (FileSizeCountKey) obj;
        return volume.equals(other.volume) &&
            bucket.equals(other.bucket) &&
            fileSizeUpperBound.equals(other.fileSizeUpperBound);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (volume + bucket + fileSizeUpperBound).hashCode();
    }
  }
}
