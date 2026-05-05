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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ExportJob} POJO.
 *
 * Focuses on the small piece of business logic baked into the model:
 * the per-job download counter and the file path -> file name derivation.
 */
class TestExportJob {

  @Test
  void downloadAllowed_initiallyTrue() {
    ExportJob job = new ExportJob("job-1", "MISSING", 3);

    assertThat(job.isDownloadAllowed()).isTrue();
    assertThat(job.getDownloadCount()).isZero();
    assertThat(job.getMaxDownloads()).isEqualTo(3);
    assertThat(job.getDownloadsRemaining()).isEqualTo(3);
  }

  @Test
  void tryReserveDownload_decrementsRemaining() {
    ExportJob job = new ExportJob("job-1", "MISSING", 3);

    assertThat(job.tryReserveDownload()).isTrue();
    assertThat(job.getDownloadCount()).isEqualTo(1);
    assertThat(job.getDownloadsRemaining()).isEqualTo(2);
    assertThat(job.isDownloadAllowed()).isTrue();
  }

  @Test
  void downloadAllowed_falseAtLimit() {
    ExportJob job = new ExportJob("job-1", "MISSING", 3);

    assertThat(job.tryReserveDownload()).isTrue();
    assertThat(job.tryReserveDownload()).isTrue();
    assertThat(job.tryReserveDownload()).isTrue();

    assertThat(job.isDownloadAllowed()).isFalse();
    assertThat(job.getDownloadsRemaining()).isZero();
    assertThat(job.tryReserveDownload()).isFalse();
  }

  @Test
  void downloadsRemaining_neverNegative() {
    ExportJob job = new ExportJob("job-1", "MISSING", 1);

    assertThat(job.tryReserveDownload()).isTrue();
    assertThat(job.tryReserveDownload()).isFalse();
    assertThat(job.tryReserveDownload()).isFalse();

    assertThat(job.getDownloadsRemaining()).isZero();
    assertThat(job.isDownloadAllowed()).isFalse();
    assertThat(job.getDownloadCount()).isEqualTo(1);
  }

  @Test
  void setFilePath_derivesFileName() {
    ExportJob job = new ExportJob("job-1", "MISSING", 3);

    job.setFilePath("/var/recon/exports/export_missing_1736000000000.tar");

    assertThat(job.getFilePath())
        .isEqualTo("/var/recon/exports/export_missing_1736000000000.tar");
    assertThat(job.getFileName()).isEqualTo("export_missing_1736000000000.tar");
  }

  @Test
  void setFilePath_null_clearsFileName() {
    ExportJob job = new ExportJob("job-1", "MISSING", 3);
    job.setFilePath("/foo/export_missing_1.tar");
    assertThat(job.getFileName()).isEqualTo("export_missing_1.tar");

    job.setFilePath(null);

    assertThat(job.getFilePath()).isNull();
    assertThat(job.getFileName()).isNull();
  }

  @Test
  void initialStatus_isQueued() {
    ExportJob job = new ExportJob("job-1", "MISSING", 3);

    assertThat(job.getStatus()).isEqualTo(ExportJob.JobStatus.QUEUED);
    assertThat(job.getSubmittedAt()).isPositive();
    assertThat(job.getEstimatedTotal()).isEqualTo(-1);
    assertThat(job.getTotalRecords()).isZero();
  }
}
