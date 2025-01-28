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

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * Class to iterate over the OM DB and populate the Recon container DB with
 * the container -&gt; Key reverse mapping.
 */
public class ContainerKeyMapperTask implements ReconOmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerKeyMapperTask.class);

  private ReconContainerMetadataManager reconContainerMetadataManager;
  private final long containerKeyFlushToDBMaxThreshold;

  @Inject
  public ContainerKeyMapperTask(ReconContainerMetadataManager
                                        reconContainerMetadataManager,
                                OzoneConfiguration configuration) {
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.containerKeyFlushToDBMaxThreshold = configuration.getLong(
        ReconServerConfigKeys.
            OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD,
        ReconServerConfigKeys.
            OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT
    );
  }

  /**
   * Read Key -> ContainerId data from OM snapshot DB and write reverse map
   * (container, key) -> count to Recon Container DB.
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    LOG.info("Starting parallel reprocess of ContainerKeyMapperTask.");

    Instant start = Instant.now();

    try {
      // Step 1: Reset Recon DB before processing
      reconContainerMetadataManager.reinitWithNewContainerDataFromOm(new HashMap<>());

      // Step 2: Thread pool for parallel execution
      int numThreads = 2; // Adjust based on performance requirements
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);

      // Step 3: List of tasks for parallel execution
      List<Future<Boolean>> futures = new ArrayList<>();
      List<BucketLayout> layouts = Arrays.asList(
          BucketLayout.LEGACY, BucketLayout.FILE_SYSTEM_OPTIMIZED
      );

      for (BucketLayout layout : layouts) {
        futures.add(executor.submit(() -> {
          LOG.info("Processing layout: {}", layout);

          Map<ContainerKeyPrefix, Integer> containerKeyMap = new HashMap<>();
          Map<Long, Long> containerKeyCountMap = new HashMap<>();

          try {
            Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable(layout);
            try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iter =
                     omKeyInfoTable.iterator()) {

              while (iter.hasNext()) {
                Table.KeyValue<String, OmKeyInfo> kv = iter.next();
                handleKeyReprocess(kv.getKey(), kv.getValue(), containerKeyMap, containerKeyCountMap);

                // Partial flush if threshold is reached
                if (containerKeyMap.size() >= containerKeyFlushToDBMaxThreshold) {
                  synchronized (this) {
                    writeToTheDB(containerKeyMap, containerKeyCountMap, Collections.emptyList());
                    containerKeyMap.clear();
                    containerKeyCountMap.clear();
                  }
                }
              }
            }

            // Final flush for any remaining data
            if (!containerKeyMap.isEmpty()) {
              synchronized (this) {
                writeToTheDB(containerKeyMap, containerKeyCountMap, Collections.emptyList());
              }
            }

            LOG.info("Completed layout: {}", layout);
            return true;
          } catch (Exception e) {
            LOG.error("Error processing layout {}", layout, e);
            return false;
          }
        }));
      }

      // Step 4: Wait for all tasks to finish
      boolean allSuccessful = true;
      for (Future<Boolean> f : futures) {
        if (!f.get()) {
          allSuccessful = false;
        }
      }

      // Step 5: Shutdown executor
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.MINUTES);

      if (!allSuccessful) {
        LOG.error("One or more threads failed during reprocess().");
        return new ImmutablePair<>(getTaskName(), false);
      }

      // Done, log total
      long durationMillis = Duration.between(start, Instant.now()).toMillis();
      LOG.info("Completed parallel reprocess in {} ms", durationMillis);
      return new ImmutablePair<>(getTaskName(), true);

    } catch (IOException ioEx) {
      LOG.error("Unable to re-init Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.error("reprocess() was interrupted.", ie);
      return new ImmutablePair<>(getTaskName(), false);
    } catch (ExecutionException ee) {
      LOG.error("Exception from parallel tasks in reprocess()", ee);
      return new ImmutablePair<>(getTaskName(), false);
    }
  }


  /**
   * For each OmKeyInfo, add container -> key mappings in local maps.
   * Return the number of keys processed (usually 1 for each key, but
   * it might be more if you consider multiple versions).
   */
  private long processOmKeyInfo(
      String keyName,
      OmKeyInfo omKeyInfo,
      Map<ContainerKeyPrefix, Integer> localKeyMap,
      Map<Long, Long> localKeyCountMap
  ) {
    long keysProcessed = 0;
    // For each version
    for (OmKeyLocationInfoGroup infoGroup : omKeyInfo.getKeyLocationVersions()) {
      long keyVersion = infoGroup.getVersion();
      for (OmKeyLocationInfo locInfo : infoGroup.getLocationList()) {
        long containerId = locInfo.getContainerID();
        ContainerKeyPrefix ckp = ContainerKeyPrefix.get(containerId, keyName, keyVersion);

        if (!localKeyMap.containsKey(ckp)) {
          localKeyMap.put(ckp, 1); // typically 1, but you can keep it general
          long oldCount = localKeyCountMap.getOrDefault(containerId, 0L);
          localKeyCountMap.put(containerId, oldCount + 1);

          // We consider this as "1 key processed" for each unique container-key prefix.
          keysProcessed++;
        }
      }
    }
    return keysProcessed;
  }

  /**
   * Flush the local maps to Recon DB in a single batch operation.
   * We do an "incremental" update for container counts since we can
   * flush partial data multiple times.
   */
  private void flushLocalMaps(
      Map<ContainerKeyPrefix, Integer> localKeyMap,
      Map<Long, Long> localKeyCountMap
  ) throws IOException {

    try (RDBBatchOperation rdbBatch = new RDBBatchOperation()) {

      // 1) Write container-key mappings
      for (Map.Entry<ContainerKeyPrefix, Integer> e : localKeyMap.entrySet()) {
        ContainerKeyPrefix prefix = e.getKey();
        int count = e.getValue();
        reconContainerMetadataManager.batchStoreContainerKeyMapping(rdbBatch, prefix, count);
      }

      // 2) Update container counts
      for (Map.Entry<Long, Long> e : localKeyCountMap.entrySet()) {
        long containerId = e.getKey();
        long localCount = e.getValue();

        // Because we are doing partial flushes, we read the old count from the DB
        // and add to it, rather than overwriting.
        long oldCount = reconContainerMetadataManager.getKeyCountForContainer(containerId);
        long newCount = oldCount + localCount;

        reconContainerMetadataManager.batchStoreContainerKeyCounts(rdbBatch, containerId, newCount);
      }

      // 3) Commit
      reconContainerMetadataManager.commitBatchOperation(rdbBatch);
    }
  }

  @Override
  public String getTaskName() {
    return "ContainerKeyMapperTask";
  }

  public Collection<String> getTaskTables() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(KEY_TABLE);
    taskTables.add(FILE_TABLE);
    return taskTables;
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    int eventCount = 0;
    final Collection<String> taskTables = getTaskTables();

    // In-memory maps for fast look up and batch write
    // (HDDS-8580) containerKeyMap map is allowed to be used
    // in "process" without batching since the maximum number of keys
    // is bounded by delta limit configurations

    // (container, key) -> count
    Map<ContainerKeyPrefix, Integer> containerKeyMap = new HashMap<>();
    // containerId -> key count
    Map<Long, Long> containerKeyCountMap = new HashMap<>();
    // List of the deleted (container, key) pair's
    List<ContainerKeyPrefix> deletedKeyCountList = new ArrayList<>();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo updatedKeyValue = omdbUpdateEvent.getValue();
      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList);
          break;

        case DELETE:
          handleDeleteOMKeyEvent(updatedKey, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList);
          break;

        case UPDATE:
          if (omdbUpdateEvent.getOldValue() != null) {
            handleDeleteOMKeyEvent(
                omdbUpdateEvent.getOldValue().getKeyName(), containerKeyMap,
                containerKeyCountMap, deletedKeyCountList);
          } else {
            LOG.warn("Update event does not have the old Key Info for {}.",
                updatedKey);
          }
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList);
          break;

        default: LOG.debug("Skipping DB update event : {}",
            omdbUpdateEvent.getAction());
        }
        eventCount++;
      } catch (IOException e) {
        LOG.error("Unexpected exception while updating key data : {} ",
            updatedKey, e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    try {
      writeToTheDB(containerKeyMap, containerKeyCountMap, deletedKeyCountList);
    } catch (IOException e) {
      LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }
    LOG.debug("{} successfully processed {} OM DB update event(s).",
        getTaskName(), eventCount);
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeToTheDB(Map<ContainerKeyPrefix, Integer> containerKeyMap,
                            Map<Long, Long> containerKeyCountMap,
                            List<ContainerKeyPrefix> deletedContainerKeyList)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      containerKeyMap.keySet().forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager
              .batchStoreContainerKeyMapping(rdbBatchOperation, key,
                  containerKeyMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });


      containerKeyCountMap.keySet().forEach((Long key) -> {
        try {
          reconContainerMetadataManager
              .batchStoreContainerKeyCounts(rdbBatchOperation, key,
                  containerKeyCountMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });

      deletedContainerKeyList.forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager
              .batchDeleteContainerMapping(rdbBatchOperation, key);
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });

      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }
  }

  /**
   * Note to delete an OM Key and update the containerID -> no. of keys counts
   * (we are preparing for batch deletion in these data structures).
   *
   * @param key key String.
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        (in this batch)
   * @param containerKeyCountMap we keep the containerKey counts in this map
   * @param deletedContainerKeyList list of the deleted containerKeys
   * @throws IOException If Unable to write to container DB.
   */
  private void handleDeleteOMKeyEvent(String key,
                                      Map<ContainerKeyPrefix, Integer>
                                          containerKeyMap,
                                      Map<Long, Long> containerKeyCountMap,
                                      List<ContainerKeyPrefix>
                                          deletedContainerKeyList)
      throws IOException {

    Set<ContainerKeyPrefix> keysToBeDeleted = new HashSet<>();
    try (TableIterator<KeyPrefixContainer, ? extends
        Table.KeyValue<KeyPrefixContainer, Integer>> keyContainerIterator =
             reconContainerMetadataManager.getKeyContainerTableIterator()) {

      // Check if we have keys in this container in the DB
      keyContainerIterator.seek(KeyPrefixContainer.get(key));
      while (keyContainerIterator.hasNext()) {
        Table.KeyValue<KeyPrefixContainer, Integer> keyValue =
            keyContainerIterator.next();
        String keyPrefix = keyValue.getKey().getKeyPrefix();
        if (keyPrefix.equals(key)) {
          if (keyValue.getKey().getContainerId() != -1) {
            keysToBeDeleted.add(keyValue.getKey().toContainerKeyPrefix());
          }
        } else {
          break;
        }
      }
    }

    // Check if we have keys in this container in our containerKeyMap
    containerKeyMap.keySet()
        .forEach((ContainerKeyPrefix containerKeyPrefix) -> {
          String keyPrefix = containerKeyPrefix.getKeyPrefix();
          if (keyPrefix.equals(key)) {
            keysToBeDeleted.add(containerKeyPrefix);
          }
        });

    for (ContainerKeyPrefix containerKeyPrefix : keysToBeDeleted) {
      deletedContainerKeyList.add(containerKeyPrefix);
      // Remove the container-key prefix from the map if we previously added
      // it in this batch (and now we delete it)
      containerKeyMap.remove(containerKeyPrefix);

      // decrement count and update containerKeyCount.
      Long containerID = containerKeyPrefix.getContainerId();
      long keyCount;
      if (containerKeyCountMap.containsKey(containerID)) {
        keyCount = containerKeyCountMap.get(containerID);
      } else {
        keyCount = reconContainerMetadataManager
            .getKeyCountForContainer(containerID);
      }
      if (keyCount > 0) {
        containerKeyCountMap.put(containerID, --keyCount);
      }
    }
  }

  /**
   * Note to add an OM key and update containerID -> no. of keys count.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        (in this batch)
   * @param containerKeyCountMap we keep the containerKey counts in this map
   * @param deletedContainerKeyList list of the deleted containerKeys
   * @throws IOException if unable to write to recon DB.
   */
  private void handlePutOMKeyEvent(String key, OmKeyInfo omKeyInfo,
                                   Map<ContainerKeyPrefix, Integer>
                                       containerKeyMap,
                                   Map<Long, Long> containerKeyCountMap,
                                   List<ContainerKeyPrefix>
                                       deletedContainerKeyList)
      throws IOException {
    long containerCountToIncrement = 0;
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo
        .getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup
          .getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(
            containerId, key, keyVersion);
        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(
            containerKeyPrefix) == 0
            && !containerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix
          // mapping again.
          containerKeyMap.put(containerKeyPrefix, 1);
          // Remove the container-key prefix from the deleted list if we
          // previously deleted it in this batch (and now we add it again)
          deletedContainerKeyList.remove(containerKeyPrefix);


          // check if container already exists and
          // increment the count of containers if it does not exist
          if (!reconContainerMetadataManager.doesContainerExists(containerId)
              && !containerKeyCountMap.containsKey(containerId)) {
            containerCountToIncrement++;
          }

          // update the count of keys for the given containerID
          long keyCount;
          if (containerKeyCountMap.containsKey(containerId)) {
            keyCount = containerKeyCountMap.get(containerId);
          } else {
            keyCount = reconContainerMetadataManager
                .getKeyCountForContainer(containerId);
          }

          // increment the count and update containerKeyCount.
          // keyCount will be 0 if containerID is not found. So, there is no
          // need to initialize keyCount for the first time.
          containerKeyCountMap.put(containerId, ++keyCount);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      reconContainerMetadataManager
          .incrementContainerCountBy(containerCountToIncrement);
    }
  }

  /**
   * Write an OM key to container DB and update containerID -> no. of keys
   * count to the Global Stats table.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        to allow incremental batching to containerKeyTable
   * @param containerKeyCountMap we keep the containerKey counts in this map
   *                             to allow batching to containerKeyCountTable
   *                             after reprocessing is done
   * @throws IOException if unable to write to recon DB.
   */
  private void handleKeyReprocess(String key,
                                  OmKeyInfo omKeyInfo,
                                  Map<ContainerKeyPrefix, Integer>
                                      containerKeyMap,
                                  Map<Long, Long> containerKeyCountMap)
      throws IOException {
    long containerCountToIncrement = 0;
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo
        .getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup
          .getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(
            containerId, key, keyVersion);
        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(
            containerKeyPrefix) == 0
            && !containerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix
          // mapping again.
          containerKeyMap.put(containerKeyPrefix, 1);

          // check if container already exists and
          // if it exists, update the count of keys for the given containerID
          // else, increment the count of containers and initialize keyCount
          long keyCount;
          if (containerKeyCountMap.containsKey(containerId)) {
            keyCount = containerKeyCountMap.get(containerId);
          } else {
            containerCountToIncrement++;
            keyCount = 0;
          }

          // increment the count and update containerKeyCount.
          containerKeyCountMap.put(containerId, ++keyCount);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      reconContainerMetadataManager
          .incrementContainerCountBy(containerCountToIncrement);
    }
  }

}
