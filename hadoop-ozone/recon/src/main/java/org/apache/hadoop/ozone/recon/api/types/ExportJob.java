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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an asynchronous CSV export job.
 */
public class ExportJob {
  
  public enum JobStatus {
    QUEUED,      // Waiting for worker thread
    RUNNING,     // Actively exporting
    COMPLETED,   // File ready for download
    FAILED       // Error occurred
  }
  
  @JsonProperty("jobId")
  private String jobId;
  
  @JsonProperty("userId")
  private String userId;
  
  @JsonProperty("state")
  private String state;
  
  @JsonProperty("limit")
  private int limit;
  
  @JsonProperty("prevKey")
  private long prevKey;
  
  @JsonProperty("status")
  private JobStatus status;
  
  @JsonProperty("submittedAt")
  private long submittedAt;
  
  @JsonProperty("startedAt")
  private long startedAt;
  
  @JsonProperty("completedAt")
  private long completedAt;
  
  @JsonProperty("totalRecords")
  private long totalRecords;
  
  @JsonProperty("estimatedTotal")
  private long estimatedTotal;
  
  @JsonProperty("filePath")
  private String filePath;
  
  @JsonProperty("errorMessage")
  private String errorMessage;
  
  @JsonProperty("progressPercent")
  private int progressPercent;
  
  @JsonProperty("queuePosition")
  private int queuePosition;

  public ExportJob(String jobId, String userId, String state, int limit, long prevKey) {
    this.jobId = jobId;
    this.userId = userId;
    this.state = state;
    this.limit = limit;
    this.prevKey = prevKey;
    this.status = JobStatus.QUEUED;
    this.submittedAt = System.currentTimeMillis();
    this.totalRecords = 0;
    this.estimatedTotal = -1;
  }

  public String getJobId() {
    return jobId;
  }

  public String getUserId() {
    return userId;
  }

  public String getState() {
    return state;
  }

  public int getLimit() {
    return limit;
  }

  public long getPrevKey() {
    return prevKey;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
    if (status == JobStatus.RUNNING && startedAt == 0) {
      startedAt = System.currentTimeMillis();
    } else if ((status == JobStatus.COMPLETED || status == JobStatus.FAILED) && completedAt == 0) {
      completedAt = System.currentTimeMillis();
    }
  }

  public long getSubmittedAt() {
    return submittedAt;
  }

  public long getStartedAt() {
    return startedAt;
  }

  public long getCompletedAt() {
    return completedAt;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public void incrementTotalRecords() {
    this.totalRecords++;
  }

  public long getEstimatedTotal() {
    return estimatedTotal;
  }

  public void setEstimatedTotal(long estimatedTotal) {
    this.estimatedTotal = estimatedTotal;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public int getProgressPercent() {
    if (estimatedTotal > 0 && totalRecords > 0) {
      return (int) ((totalRecords * 100) / estimatedTotal);
    }
    return 0;
  }
  
  public int getQueuePosition() {
    return queuePosition;
  }
  
  public void setQueuePosition(int queuePosition) {
    this.queuePosition = queuePosition;
  }
}
