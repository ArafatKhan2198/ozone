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
import java.util.Arrays;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
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
  private final ConcurrentHashMap<FileSizeCountKey, Long> sharedFileSizeCountMap = new ConcurrentHashMap<>();
  private final ExecutorService executorService;

  @Inject
  public FileSizeCountTask(FileCountBySizeDao fileCountBySizeDao,
                           UtilizationSchemaDefinition utilizationSchemaDefinition,
                           ExecutorService executorService) {
    this.fileCountBySizeDao = fileCountBySizeDao;
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
    this.executorService = executorService;
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
    LOG.info("Starting reprocess of FileSizeCountTask...");
    long startTime = System.currentTimeMillis();

    // Truncate table first
    int execute = dslContext.delete(FILE_COUNT_BY_SIZE).execute();
    LOG.debug("Cleared {} existing records from {}", execute, FILE_COUNT_BY_SIZE);

    List<Future<Boolean>> futures = Arrays.asList(
        submitReprocessTask("FSO", BucketLayout.FILE_SYSTEM_OPTIMIZED, omMetadataManager),
        submitReprocessTask("LEGACY", BucketLayout.LEGACY, omMetadataManager)
    );

    boolean allSuccess = true;
    try {
      for (Future<Boolean> future : futures) {
        if (!future.get()) {
          allSuccess = false;
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Parallel processing failed: ", e);
      allSuccess = false;
      Thread.currentThread().interrupt();
    }

    // Write any remaining entries to the database
    if (!sharedFileSizeCountMap.isEmpty()) {
      writeCountsToDB(sharedFileSizeCountMap);
      sharedFileSizeCountMap.clear();
    }

    LOG.info("Reprocess completed. Success: {}. Time taken: {} ms",
        allSuccess, (System.currentTimeMillis() - startTime));
    return new ImmutablePair<>(getTaskName(), allSuccess);
  }

  /**
   * Submits a reprocess task with proper thread naming.
   */
  private Future<Boolean> submitReprocessTask(String bucketType, BucketLayout layout,
                                              OMMetadataManager omMetadataManager) {
    return executorService.submit(() -> {
      Thread currentThread = Thread.currentThread();
      String originalName = currentThread.getName();
      try {
        currentThread.setName("FileSizeCountTask-" + bucketType + "-" + originalName);
        return reprocessBucketLayout(layout, omMetadataManager);
      } finally {
        currentThread.setName(originalName); // Restore original name after execution
      }
    });
  }

  private Boolean reprocessBucketLayout(BucketLayout bucketLayout,
                                      OMMetadataManager omMetadataManager) {
    long keysProcessed = 0;

    try {
      Table<String, OmKeyInfo> omKeyInfoTable =
          omMetadataManager.getKeyTable(bucketLayout);
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               keyIter = omKeyInfoTable.iterator()) {
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          FileSizeCountKey key = getFileSizeCountKey(kv.getValue());

          // Atomically update the count in the shared map
          sharedFileSizeCountMap.merge(key, 1L, Long::sum);
          keysProcessed++;

          // Periodically write to the database to avoid memory overflow
          if (sharedFileSizeCountMap.size() >= 100_000) {
            writeCountsToDB(sharedFileSizeCountMap);
            sharedFileSizeCountMap.clear();
          }
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Failed to process {} layout: ", bucketLayout, ioEx);
      return false;
    }
    return true;
  }

  @Override
  public String getTaskName() {
    return "FileSizeCountTask";
  }

  public Collection<String> getTaskTables() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(KEY_TABLE);
    taskTables.add(FILE_TABLE);
    return taskTables;
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

    long startTime = System.currentTimeMillis();
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, Object> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
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
              LOG.warn("Update event does not have the old keyInfo for {}.",
                  updatedKey);
            }
            break;

          default:
            LOG.trace("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception while processing key {}.",
              updatedKey, e);
          return new ImmutablePair<>(getTaskName(), false);
        }
      } else {
        LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
            value.getClass().getName(), updatedKey);
      }
    }
    if (!fileSizeCountMap.isEmpty()) {
      writeCountsToDB(fileSizeCountMap);
    }
    LOG.debug("{} successfully processed in {} milliseconds",
        getTaskName(), (System.currentTimeMillis() - startTime));
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * Populate DB with the counts of file sizes calculated
   * using the dao.
   *
   */
  private void writeCountsToDB(Map<FileSizeCountKey, Long> fileSizeCountMap) {

    List<FileCountBySize> insertToDb = new ArrayList<>();
    List<FileCountBySize> updateInDb = new ArrayList<>();
    boolean isDbTruncated = isFileCountBySizeTableEmpty(); // Check if table is empty
    fileSizeCountMap.forEach((key, count) -> {
      FileCountBySize newRecord = new FileCountBySize();
      newRecord.setVolume(key.volume);
      newRecord.setBucket(key.bucket);
      newRecord.setFileSize(key.fileSizeUpperBound);
      newRecord.setCount(count);

      if (!isDbTruncated) {
        // Get the current count from the database and update
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
          // Insert new row only for non-zero counts.
          insertToDb.add(newRecord);
        } else if (fileCountRecord != null) {
          newRecord.setCount(fileCountRecord.getCount() + count);
          updateInDb.add(newRecord);
        }
      } else if (newRecord.getCount() > 0) {
        // Insert new row only for non-zero counts.
        insertToDb.add(newRecord);
      }
    });

    // Perform batch inserts and updates
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

  /**
   * Checks if the FILE_COUNT_BY_SIZE table is empty.
   *
   * @return true if the table is empty, false otherwise.
   */
  private boolean isFileCountBySizeTableEmpty() {
    return dslContext.fetchCount(FILE_COUNT_BY_SIZE) == 0;
  }

  private static class BucketLayoutProcessResult {
    private final boolean success;
    private final long keysProcessed;

    BucketLayoutProcessResult(boolean success, long keysProcessed) {
      this.success = success;
      this.keysProcessed = keysProcessed;
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
