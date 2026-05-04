/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.api.types.ExportJob;
import org.apache.hadoop.ozone.recon.api.types.ExportJob.JobStatus;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages asynchronous CSV export jobs.
 */
@Singleton
public class ExportJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExportJobManager.class);
  private static final int MAX_QUEUE_SIZE = 4;
  
  private final Map<String, ExportJob> jobTracker = new ConcurrentHashMap<>();
  private final LinkedHashMap<String, ExportJob> jobQueue = new LinkedHashMap<>();
  private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
  private final ExecutorService workerPool;
  private final ContainerHealthSchemaManager containerHealthSchemaManager;
  private final String exportDirectory;

  @Inject
  public ExportJobManager(ContainerHealthSchemaManager containerHealthSchemaManager,
                          OzoneConfiguration conf) {
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    
    // Use single thread executor for sequential processing (no concurrent DB access)
    this.workerPool = Executors.newSingleThreadExecutor();
    
    this.exportDirectory = conf.get(
        ReconServerConfigKeys.OZONE_RECON_EXPORT_DIRECTORY,
        ReconServerConfigKeys.OZONE_RECON_EXPORT_DIRECTORY_DEFAULT);
    
    // Create export directory if it doesn't exist
    try {
      Files.createDirectories(Paths.get(exportDirectory));
    } catch (IOException e) {
      LOG.error("Failed to create export directory: {}", exportDirectory, e);
    }
    
    LOG.info("ExportJobManager initialized with single-threaded queue (max {} jobs)", MAX_QUEUE_SIZE);
  }

  public synchronized String submitJob(String userId, String state, int limit, long prevKey) {
    // Reject duplicate: same state already queued or running
    boolean stateAlreadyActive = jobQueue.values().stream().anyMatch(j -> j.getState().equals(state)) ||
        jobTracker.values().stream().anyMatch(j -> j.getState().equals(state) && j.getStatus() == JobStatus.RUNNING);
    if (stateAlreadyActive) {
      throw new IllegalStateException(
          "An export for state " + state + " is already queued or running. Please wait for it to complete.");
    }

    // Check global queue size limit
    synchronized (jobQueue) {
      if (jobQueue.size() >= MAX_QUEUE_SIZE) {
        throw new IllegalStateException(
            "Export queue is full (max " + MAX_QUEUE_SIZE + " jobs). Please try again later.");
      }
    }
    
    String jobId = UUID.randomUUID().toString();
    ExportJob job = new ExportJob(jobId, userId, state, limit, prevKey);
    // Filename format: export_{state}_{userId}_{shortJobId}.tar
    String shortJobId = jobId.substring(0, 8);
    String filePath = exportDirectory + "/export_" + state.toLowerCase() + "_" + userId + "_" + shortJobId + ".tar";
    job.setFilePath(filePath);
    
    jobTracker.put(jobId, job);
    
    // Add to queue (LinkedHashMap maintains insertion order)
    synchronized (jobQueue) {
      jobQueue.put(jobId, job);
    }
    
    // Submit to single-threaded worker pool
    Future<?> future = workerPool.submit(() -> executeExport(job));
    runningTasks.put(jobId, future);
    
    int queuePosition = getQueuePosition(jobId);
    LOG.info("Submitted export job {} for user {} (state={}, limit={}, queue position={})",
        jobId, userId, state, limit, queuePosition);
    
    return jobId;
  }

  public ExportJob getJob(String jobId) {
    return jobTracker.get(jobId);
  }

  /**
   * Returns all tracked export jobs (any status).
   */
  public List<ExportJob> getAllJobs() {
    return new ArrayList<>(jobTracker.values());
  }
  
  /**
   * Get the queue position for a job (1-indexed).
   * Returns 0 if job is not in queue (running, completed, or not found).
   */
  public int getQueuePosition(String jobId) {
    synchronized (jobQueue) {
      if (!jobQueue.containsKey(jobId)) {
        return 0;
      }
      
      int position = 1;
      for (String id : jobQueue.keySet()) {
        if (id.equals(jobId)) {
          return position;
        }
        position++;
      }
      return 0;
    }
  }

  public void cancelJob(String jobId) {
    ExportJob job = jobTracker.get(jobId);
    if (job == null) {
      throw new IllegalStateException("Job not found: " + jobId);
    }
    
    if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED) {
      throw new IllegalStateException("Job already completed or failed");
    }
    
    // Remove from queue if still queued
    synchronized (jobQueue) {
      jobQueue.remove(jobId);
    }
    
    Future<?> future = runningTasks.get(jobId);
    if (future != null) {
      future.cancel(true);
      runningTasks.remove(jobId);
    }
    
    job.setStatus(JobStatus.FAILED);
    job.setErrorMessage("Cancelled by user");
    
    // Delete partial files/directory
    deleteDirectory(Paths.get(exportDirectory + "/" + jobId));
    deleteFileQuietly(job.getFilePath());
    
    LOG.info("Cancelled export job {}", jobId);
  }

  private void executeExport(ExportJob job) {
    String jobDirectory = exportDirectory + "/" + job.getJobId();
    Path jobDir = Paths.get(jobDirectory);
    String tarFilePath = job.getFilePath();  // Use the filename set in submitJob
    
    try {
      // Create job-specific directory for CSV files
      Files.createDirectories(jobDir);
      
      // Remove from queue and mark as running
      synchronized (jobQueue) {
        jobQueue.remove(job.getJobId());
      }
      job.setStatus(JobStatus.RUNNING);
      LOG.info("Starting export job {}", job.getJobId());
      
      ContainerSchemaDefinition.UnHealthyContainerStates internalState =
          ContainerSchemaDefinition.UnHealthyContainerStates.valueOf(job.getState());
      
      // Get total count first for progress tracking
      long estimatedTotal = containerHealthSchemaManager.getUnhealthyContainersCount(
          internalState, job.getLimit(), job.getPrevKey());
      job.setEstimatedTotal(estimatedTotal);
      LOG.info("Export job {} will process approximately {} records", job.getJobId(), estimatedTotal);
      
      // Open database cursor
      try (Cursor<UnhealthyContainersRecord> cursor =
               containerHealthSchemaManager.getUnhealthyContainersCursor(
                   internalState, job.getLimit(), job.getPrevKey())) {
        
        int fileIndex = 1;
        long totalRecords = 0;
        long recordsInCurrentFile = 0;
        final int CHUNK_SIZE = 500_000;
        
        BufferedWriter writer = null;
        FileOutputStream fos = null;
        
        try {
          while (cursor.hasNext()) {
            // Check for cancellation
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException("Job cancelled");
            }
            
            // Start new CSV file if needed
            if (recordsInCurrentFile == 0) {
              // Close previous file if exists
              if (writer != null) {
                writer.flush();
                writer.close();
              }
              
              String csvFileName = String.format("%s/unhealthy_containers_%s_part%03d.csv",
                  jobDirectory, job.getState().toLowerCase(), fileIndex);
              fos = new FileOutputStream(csvFileName);
              writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
              
              // Write CSV header
              writer.write("container_id,container_state,in_state_since," +
                  "expected_replica_count,actual_replica_count,replica_delta\n");
              
              LOG.info("Created CSV file: part{}", fileIndex);
            }
            
            // Fetch and write record
            UnhealthyContainersRecord rec = cursor.fetchNext();
            StringBuilder sb = new StringBuilder(128);
            sb.append(rec.getContainerId()).append(',')
                .append(rec.getContainerState()).append(',')
                .append(rec.getInStateSince()).append(',')
                .append(rec.getExpectedReplicaCount()).append(',')
                .append(rec.getActualReplicaCount()).append(',')
                .append(rec.getReplicaDelta()).append('\n');
            writer.write(sb.toString());
            
            totalRecords++;
            recordsInCurrentFile++;
            job.setTotalRecords(totalRecords);
            
            // Move to next file if chunk limit reached
            if (recordsInCurrentFile >= CHUNK_SIZE) {
              writer.flush();
              writer.close();
              writer = null;
              recordsInCurrentFile = 0;
              fileIndex++;
            }
            
            // Flush every 10K rows
            if (recordsInCurrentFile > 0 && recordsInCurrentFile % 10000 == 0) {
              writer.flush();
            }
          }
          
          // Close last file
          if (writer != null) {
            writer.flush();
            writer.close();
          }
          
        } finally {
          if (writer != null) {
            try {
              writer.close();
            } catch (IOException e) {
              LOG.warn("Error closing writer", e);
            }
          }
        }
        
        LOG.info("Export job {} wrote {} records across {} files",
            job.getJobId(), totalRecords, fileIndex);
        
        // Create TAR archive
        File tarFile = new File(tarFilePath);
        Archiver.create(tarFile, jobDir);
        LOG.info("Created TAR archive: {}", tarFilePath);
        
        // Delete CSV files and job directory
        deleteDirectory(jobDir);
        LOG.info("Deleted temporary CSV files for job {}", job.getJobId());
        
        // Update job with TAR file path
        job.setFilePath(tarFilePath);
        job.setStatus(JobStatus.COMPLETED);
        LOG.info("Completed export job {} ({} records)", job.getJobId(), totalRecords);
        
      } catch (InterruptedException e) {
        job.setStatus(JobStatus.FAILED);
        job.setErrorMessage("Job was cancelled");
        deleteDirectory(jobDir);
        deleteFileQuietly(tarFilePath);
        LOG.info("Export job {} was cancelled", job.getJobId());
        Thread.currentThread().interrupt();
      }
      
    } catch (Exception e) {
      job.setStatus(JobStatus.FAILED);
      job.setErrorMessage(e.getMessage());
      deleteDirectory(jobDir);
      deleteFileQuietly(tarFilePath);
      LOG.error("Export job {} failed", job.getJobId(), e);
    } finally {
      // 3-second cooldown before the next queued job is picked up by the single worker thread.
      try {
        Thread.sleep(3000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      runningTasks.remove(job.getJobId());
    }
  }
  
  private void deleteDirectory(Path directory) {
    try {
      if (Files.exists(directory)) {
        Files.walk(directory)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete directory: {}", directory, e);
    }
  }
  
  private void deleteFileQuietly(String filePath) {
    try {
      Files.deleteIfExists(Paths.get(filePath));
    } catch (IOException e) {
      LOG.warn("Failed to delete file: {}", filePath, e);
    }
  }

  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down ExportJobManager");
    workerPool.shutdownNow();
    try {
      workerPool.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Timeout waiting for executor shutdown", e);
      Thread.currentThread().interrupt();
    }
  }
}
