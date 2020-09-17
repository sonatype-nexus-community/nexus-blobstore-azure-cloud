/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2019-present Sonatype, Inc.
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

import java.util.Locale;

import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.goodies.common.ComponentSupport;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.ACCOUNT_KEY_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.ACCOUNT_NAME_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CONFIG_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CONTAINER_NAME_KEY;

/**
 * Creates azure client with settings from configuration
 */
@Named
public class AzureStorageClientFactory
    extends ComponentSupport
{
  private final int chunkSize;

  private final int copyTimeout;

  private final int listBlobsTimeout;

  @Inject
  public AzureStorageClientFactory(@Named("${nexus.azure.blocksize:-5242880}") final int chunkSize,
                                   @Named("${nexus.azure.copyTimeout_sec:-30}") final int copyTimeout,
                                   @Named("${nexus.azure.listBlobsTimout_sec:-30}") final int listBlobsTimeout)
  {
    this.chunkSize = chunkSize;
    this.copyTimeout = copyTimeout;
    this.listBlobsTimeout = listBlobsTimeout;
  }

  public AzureClient create(final BlobStoreConfiguration blobStoreConfiguration) throws Exception {
    String accountName = blobStoreConfiguration.attributes(CONFIG_KEY).get(ACCOUNT_NAME_KEY, String.class);
    String accountKey = blobStoreConfiguration.attributes(CONFIG_KEY).get(ACCOUNT_KEY_KEY, String.class);
    String containerName = blobStoreConfiguration.attributes(CONFIG_KEY).get(CONTAINER_NAME_KEY, String.class);
    StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
    String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
    BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential)
        .buildClient();
    return new AzureClientImpl(storageClient, containerName, chunkSize, copyTimeout, listBlobsTimeout);
  }
}
