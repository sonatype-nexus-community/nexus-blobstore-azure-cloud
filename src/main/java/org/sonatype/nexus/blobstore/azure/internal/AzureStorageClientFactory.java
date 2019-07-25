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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidKeyException;

import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.goodies.common.ComponentSupport;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.rest.v2.http.HttpPipeline;

import static com.microsoft.azure.storage.blob.StorageURL.createPipeline;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.ACCOUNT_KEY_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.ACCOUNT_NAME_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CLIENT_TYPE;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CONFIG_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CONTAINER_NAME_KEY;

/**
 * Creates azure client with settings from configuration
 */
@Named
public class AzureStorageClientFactory
    extends ComponentSupport
{
  private int chunkSize;

  public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

  @Inject
  public AzureStorageClientFactory(@Named("${nexus.azure.blocksize:-5242880}") final int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public AzureClient create(final BlobStoreConfiguration blobStoreConfiguration)
      throws Exception
  {
    String clientType = blobStoreConfiguration.attributes(CONFIG_KEY).get(CLIENT_TYPE, String.class);
    String accountName = blobStoreConfiguration.attributes(CONFIG_KEY).get(ACCOUNT_NAME_KEY, String.class);
    String accountKey = blobStoreConfiguration.attributes(CONFIG_KEY).get(ACCOUNT_KEY_KEY, String.class);
    String containerName = blobStoreConfiguration.attributes(CONFIG_KEY).get(CONTAINER_NAME_KEY, String.class);
    if ("sync".equalsIgnoreCase(clientType)) {
      return getSyncAzureClient(accountName, accountKey, containerName);
    }
    return getReactiveAzureClient(accountName, accountKey, containerName);
  }

  private ReactiveAzureClient getReactiveAzureClient(final String accountName,
                                                     final String accountKey, final String containerName)
      throws InvalidKeyException, MalformedURLException
  {
    HttpPipeline pipeline = createPipeline(new SharedKeyCredentials(accountName, accountKey));
    URL url = new URL(format(ROOT, "https://%s.blob.core.windows.net", accountName));
    ServiceURL serviceURL = new ServiceURL(url, pipeline);
    ContainerURL containerURL = serviceURL.createContainerURL(containerName);
    return new ReactiveAzureClient(containerURL, chunkSize, containerName);
  }

  private SyncAzureClient getSyncAzureClient(final String accountName,
                                             final String accountKey, final String containerName)
      throws InvalidKeyException, URISyntaxException
  {
    CloudStorageAccount account =
        CloudStorageAccount.parse(String.format(storageConnectionString, accountName, accountKey));
    CloudBlobClient serviceClient = account.createCloudBlobClient();

    return new SyncAzureClient(serviceClient, chunkSize, containerName);
  }
}
