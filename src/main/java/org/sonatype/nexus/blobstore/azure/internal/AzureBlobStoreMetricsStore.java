/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2017-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.nexus.blobstore.AccumulatingBlobStoreMetrics;
import org.sonatype.nexus.blobstore.BlobStoreMetricsStoreSupport;
import org.sonatype.nexus.blobstore.quota.BlobStoreQuotaService;
import org.sonatype.nexus.common.node.NodeAccess;
import org.sonatype.nexus.scheduling.PeriodicJobService;

import com.google.common.collect.ImmutableMap;

import static com.google.common.base.Preconditions.checkNotNull;

@Named
public class AzureBlobStoreMetricsStore
    extends BlobStoreMetricsStoreSupport<AzurePropertiesFile>
{
  private static final Map<String, Long> AVAILABLE_SPACE_BY_FILE_STORE = ImmutableMap
      .of(AzureBlobStore.CONFIG_KEY, Long.MAX_VALUE);

  private final NodeAccess nodeAccess;

  private AzureClient azureClient;

  @Inject
  public AzureBlobStoreMetricsStore(final NodeAccess nodeAccess,
                                    final PeriodicJobService jobService,
                                    final BlobStoreQuotaService quotaService,
                                    @Named("${nexus.blobstore.quota.warnIntervalSeconds:-60}")
                                    final int quotaCheckInterval)
  {
    super(nodeAccess, jobService, quotaService, quotaCheckInterval);
    this.nodeAccess = checkNotNull(nodeAccess);
  }

  public void setAzureClient(AzureClient azureClient) {
    this.azureClient = azureClient;
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();

    azureClient = null;
  }

  @Override
  protected AzurePropertiesFile getProperties() {
    return new AzurePropertiesFile(azureClient, nodeAccess.getId() + "-" + METRICS_FILENAME);
  }

  @Override
  protected AccumulatingBlobStoreMetrics getAccumulatingBlobStoreMetrics() {
    return new AccumulatingBlobStoreMetrics(0, 0, AVAILABLE_SPACE_BY_FILE_STORE, true);
  }

  @Override
  protected Stream<AzurePropertiesFile> backingFiles() {
    if (azureClient == null) {
      return Stream.empty();
    }
    return Stream.of(getProperties());
    //return StreamSupport.stream(azureClient
    //    .listFiles("", s -> s.endsWith(METRICS_SUFFIX))
    //    .map(x -> new AzurePropertiesFile(azureClient, x))
    //    .blockingIterable().spliterator(), false);
  }

  public void remove() {
    backingFiles().forEach(metricsFile -> {
      try {
        metricsFile.remove();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
