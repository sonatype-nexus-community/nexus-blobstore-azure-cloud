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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.nexus.blobstore.BlobIdLocationResolver;
import org.sonatype.nexus.blobstore.BlobStoreSupport;
import org.sonatype.nexus.blobstore.BlobSupport;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobAttributes;
import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.api.BlobStore;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.blobstore.api.BlobStoreException;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;
import org.sonatype.nexus.blobstore.api.BlobStoreUsageChecker;
import org.sonatype.nexus.common.log.DryRunPrefix;
import org.sonatype.nexus.common.stateguard.Guarded;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.HashCode;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.cache.CacheLoader.from;
import static org.sonatype.nexus.blobstore.api.BlobAttributesConstants.HEADER_PREFIX;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.FAILED;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.NEW;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STARTED;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STOPPED;

/**
 * Azure Cloud Storage backed {@link BlobStore}.
 */
@Named(AzureBlobStore.TYPE)
public class AzureBlobStore
    extends BlobStoreSupport
{
  public static final String TYPE = "Azure Cloud Storage";

  public static final String CONFIG_KEY = TYPE.toLowerCase();

  public static final String ACCOUNT_NAME_KEY = "account_name";

  public static final String ACCOUNT_KEY_KEY = "account_key";

  public static final String CONTAINER_NAME_KEY = "container_name";

  public static final String BLOB_CONTENT_SUFFIX = ".bytes";

  public static final String BLOB_ATTRIBUTE_SUFFIX = ".properties";

  static final String CONTENT_PREFIX = "content";

  private AzureStorageClientFactory azureStorageClientFactory;

  private final BlobIdLocationResolver blobIdLocationResolver;

  private final AzureBlobStoreMetricsStore metricsStore;

  private final DryRunPrefix dryRunPrefix;

  private AzureClient azureClient;

  private LoadingCache<BlobId, AzureBlob> liveBlobs;

  @Inject
  public AzureBlobStore(final AzureStorageClientFactory azureStorageClientFactory,
                        final BlobIdLocationResolver blobIdLocationResolver,
                        final AzureBlobStoreMetricsStore metricsStore,
                        final DryRunPrefix dryRunPrefix)
  {
    super(blobIdLocationResolver, dryRunPrefix);
    this.azureStorageClientFactory = checkNotNull(azureStorageClientFactory);
    this.blobIdLocationResolver = checkNotNull(blobIdLocationResolver);
    this.metricsStore = metricsStore;
    this.dryRunPrefix = dryRunPrefix;
  }

  @Override
  protected void doStart() throws Exception {
    log.info("starting");
    liveBlobs = CacheBuilder.newBuilder().weakValues().build(from(AzureBlob::new));

    metricsStore.start();
  }


  @Override
  protected void doStop() throws Exception {
    liveBlobs = null;
    metricsStore.stop();
  }

  @Override
  @Guarded(by = STARTED)
  public Blob create(final Path path, final Map<String, String> map, final long size, final HashCode hash) {
    throw new BlobStoreException("hard links not supported", null);
  }

  @Override
  protected Blob doCreate(final InputStream inputStream,
                          final Map<String, String> headers,
                          @Nullable final BlobId assignedBlobId)
  {
    checkNotNull(inputStream);
    final BlobId blobId = getBlobId(headers, assignedBlobId);
    final String blobPath = contentPath(blobId);

    final AzureBlob blob = liveBlobs.getUnchecked(blobId);

    blob.lock();

    try {
      azureClient.create(blobPath, inputStream);

    }
    catch (IOException e) {
      new BlobStoreException(e, blobId);
    }
    return null;
  }

  @Override
  @Guarded(by = STARTED)
  public Blob copy(final BlobId blobId, final Map<String, String> headers) {
    return null;
  }

  @Nullable
  @Override
  @Guarded(by = STARTED)
  public Blob get(final BlobId blobId) {
    return get(blobId, false);
  }

  @Nullable
  @Override
  public Blob get(final BlobId blobId, final boolean includeDeleted) {
    checkNotNull(blobId);

    final AzureBlob blob = liveBlobs.getUnchecked(blobId);

    if(blob.isStale()) {
      Lock lock = blob.lock();
      try {
        if(blob.isStale()) {
          AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, "");
          boolean loaded = blobAttributes.load();
          if (!loaded) {
            log.warn("Attempt to access non-existent blob {} ({})", blobId, blobAttributes);
            return null;
          }

          if (blobAttributes.isDeleted() && !includeDeleted) {
            log.warn("Attempt to access soft-deleted blob {} ({})", blobId, blobAttributes);
            return null;
          }

          blob.refresh(blobAttributes.getHeaders(), blobAttributes.getMetrics());
        }
      }
      catch (IOException e) {
        throw new BlobStoreException(e, blobId);
      }
      finally {
        lock.unlock();
      }
    }

    log.debug("Accessing blob {}", blobId);

    return blob;
  }


  @Override
  @Guarded(by = STARTED)
  public boolean delete(final BlobId blobId, final String reason) {
    checkNotNull(blobId);
    throw new UnsupportedOperationException("fixme");
  }

  @Override
  protected boolean doDelete(final BlobId blobId, final String s) {
    throw new UnsupportedOperationException("fixme");
  }

  @Override
  @Guarded(by = STARTED)
  public boolean deleteHard(final BlobId blobId) {
    checkNotNull(blobId);
    throw new UnsupportedOperationException("fixme");
  }

  @Override
  @Guarded(by = STARTED)
  public BlobStoreMetrics getMetrics() {
    return metricsStore.getMetrics();
  }

  @Override
  @Guarded(by = STARTED)
  public void compact() {
    compact(null);
  }

  @Override
  @Guarded(by = STARTED)
  public void compact(@Nullable final BlobStoreUsageChecker blobStoreUsageChecker) {

  }

  @Override
  public BlobStoreConfiguration getBlobStoreConfiguration() {
    return this.blobStoreConfiguration;
  }

  @Override
  protected void doInit(final BlobStoreConfiguration blobStoreConfiguration) {
    try {
      azureClient = azureStorageClientFactory.create(blobStoreConfiguration);
      metricsStore.setAzureClient(azureClient);
    }
    catch (MalformedURLException | InvalidKeyException e) {
      throw new BlobStoreException("Unable to initialize blob store container", e, null);
    }
  }

  @Override
  @Guarded(by = {NEW, STOPPED, FAILED})
  public void remove() {
    // TODO delete bucket only if it is empty
  }

  @Override
  @Guarded(by = STARTED)
  public Stream<BlobId> getBlobIdStream() {
    throw new UnsupportedOperationException("fixme");
  }

  @Override
  @Guarded(by = STARTED)
  public Stream<BlobId> getDirectPathBlobIdStream(final String prefix) {
    throw new UnsupportedOperationException("fixme");
  }

  /**
   * @return the {@link BlobAttributes} for the blob, or null
   * @throws BlobStoreException if an {@link IOException} occurs
   */
  @Override
  @Guarded(by = STARTED)
  public BlobAttributes getBlobAttributes(final BlobId blobId) {
    throw new UnsupportedOperationException("fixme");
  }

  @Override
  @Guarded(by = STARTED)
  public void setBlobAttributes(final BlobId blobId, final BlobAttributes blobAttributes) {
    AzureBlobAttributes existing = (AzureBlobAttributes) getBlobAttributes(blobId);
    if (existing != null) {
      try {
        existing.updateFrom(blobAttributes);
        existing.store();
      }
      catch (IOException e) {
        log.error("Unable to set AzureBlobAttributes for blob id: {}", blobId, e);
      }
    }
  }

  /**
   * @return true if a blob exists in the store with the provided {@link BlobId}
   * @throws BlobStoreException if an IOException occurs
   */
  @Override
  @Guarded(by = STARTED)
  public boolean exists(final BlobId blobId) {
    checkNotNull(blobId);
    return getBlobAttributes(blobId) != null;
  }

  @Override
  @Guarded(by = STARTED)
  public boolean undelete(@Nullable final BlobStoreUsageChecker blobStoreUsageChecker,
                          final BlobId blobId,
                          final BlobAttributes attributes,
                          final boolean isDryRun)
  {
    checkNotNull(attributes);
    String logPrefix = isDryRun ? dryRunPrefix.get() : "";
    Optional<String> blobName = Optional.of(attributes)
        .map(BlobAttributes::getProperties)
        .map(p -> p.getProperty(HEADER_PREFIX + BLOB_NAME_HEADER));
    if (!blobName.isPresent()) {
      log.error("Property not present: {}, for blob id: {}, at path: {}", HEADER_PREFIX + BLOB_NAME_HEADER,
          blobId, attributePath(blobId));
      return false;
    }
    if (attributes.isDeleted() && blobStoreUsageChecker != null &&
        blobStoreUsageChecker.test(this, blobId, blobName.get())) {
      String deletedReason = attributes.getDeletedReason();
      if (!isDryRun) {
        attributes.setDeleted(false);
        attributes.setDeletedReason(null);
        try {
          attributes.store();
        }
        catch (IOException e) {
          log.error("Error while un-deleting blob id: {}, deleted reason: {}, blob store: {}, blob name: {}",
              blobId, deletedReason, blobStoreConfiguration.getName(), blobName.get(), e);
        }
      }
      log.warn(
          "{}Soft-deleted blob still in use, un-deleting blob id: {}, deleted reason: {}, blob store: {}, blob name: {}",
          logPrefix, blobId, deletedReason, blobStoreConfiguration.getName(), blobName.get());
      return true;
    }
    return false;
  }

  @Override
  protected String attributePathString(final BlobId blobId) {
    return null;
  }

  @Override
  @Guarded(by = STARTED)
  public boolean isWritable() {
    return false;
  }

  /**
   * Returns path for blob-id content file relative to root directory.
   */
  private String contentPath(final BlobId id) {
    return getLocation(id) + BLOB_CONTENT_SUFFIX;
  }

  /**
   * Returns path for blob-id attribute file relative to root directory.
   */
  private String attributePath(final BlobId id) {
    return getLocation(id) + BLOB_ATTRIBUTE_SUFFIX;
  }

  /**
   * Returns the location for a blob ID based on whether or not the blob ID is for a temporary or permanent blob.
   */
  private String getLocation(final BlobId id) {
    return CONTENT_PREFIX + "/" + blobIdLocationResolver.getLocation(id);
  }

  class AzureBlob
      extends BlobSupport
  {
    public AzureBlob(final BlobId blobId) {
      super(blobId);
    }

    @Override
    public InputStream getInputStream() {
      return azureClient.get(contentPath(getId()));
    }
  }
}
