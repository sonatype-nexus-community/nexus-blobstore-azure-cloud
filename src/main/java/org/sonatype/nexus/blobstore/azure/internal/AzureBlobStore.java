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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.nexus.blobstore.AttributesLocation;
import org.sonatype.nexus.blobstore.BlobIdLocationResolver;
import org.sonatype.nexus.blobstore.BlobStoreSupport;
import org.sonatype.nexus.blobstore.BlobSupport;
import org.sonatype.nexus.blobstore.MetricsInputStream;
import org.sonatype.nexus.blobstore.StreamMetrics;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobAttributes;
import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.api.BlobMetrics;
import org.sonatype.nexus.blobstore.api.BlobStore;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.blobstore.api.BlobStoreException;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;
import org.sonatype.nexus.blobstore.api.BlobStoreUsageChecker;
import org.sonatype.nexus.common.log.DryRunPrefix;
import org.sonatype.nexus.common.stateguard.Guarded;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import io.reactivex.Observable;
import io.reactivex.functions.Predicate;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.from;
import static org.sonatype.nexus.blobstore.DirectPathLocationStrategy.DIRECT_PATH_ROOT;
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
    extends BlobStoreSupport<AttributesLocation>
{
  public static final String TYPE = "Azure Cloud Storage";

  public static final String CONFIG_KEY = TYPE.toLowerCase();

  public static final String ACCOUNT_NAME_KEY = "account_name";

  public static final String ACCOUNT_KEY_KEY = "account_key";

  public static final String CONTAINER_NAME_KEY = "container_name";

  public static final String BLOB_CONTENT_SUFFIX = ".bytes";

  public static final String BLOB_ATTRIBUTE_SUFFIX = ".properties";

  public static final String METADATA_FILENAME = "metadata.properties";

  static final String CONTENT_PREFIX = "content";

  public static final String DIRECT_PATH_PREFIX = CONTENT_PREFIX + "/" + DIRECT_PATH_ROOT;

  public static final String TYPE_KEY = "type";

  public static final String TYPE_V1 = "azure/1";

  private AzureStorageClientFactory azureStorageClientFactory;

  private final BlobIdLocationResolver blobIdLocationResolver;

  private final AzureBlobStoreMetricsStore storeMetrics;

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
    this.storeMetrics = metricsStore;
    this.dryRunPrefix = dryRunPrefix;
  }

  @Override
  protected void doStart() throws Exception {
    log.info("starting");
    AzurePropertiesFile metadata = new AzurePropertiesFile(azureClient, METADATA_FILENAME);
    if (metadata.exists()) {
      metadata.load();
      String type = metadata.getProperty(TYPE_KEY);
      checkState(TYPE_V1.equals(type), "Unsupported blob store type/version: %s in %s", type,
          metadata);
    }
    else {
      // assumes new blobstore, write out type
      metadata.setProperty(TYPE_KEY, TYPE_V1);
      metadata.store();
    }
    liveBlobs = CacheBuilder.newBuilder().weakValues().build(from(AzureBlob::new));
    storeMetrics.setAzureClient(azureClient);
    storeMetrics.setBlobStore(this);
    storeMetrics.start();
  }


  @Override
  protected void doStop() throws Exception {
    liveBlobs = null;
    storeMetrics.stop();
  }

  @Override
  @Guarded(by = STARTED)
  public Blob create(final Path path, final Map<String, String> map, final long size, final HashCode hash) {
    throw new BlobStoreException("hard links not supported", null);
  }

  @Override
  protected Blob doCreate(final InputStream blobData,
                          final Map<String, String> headers,
                          @Nullable final BlobId blobId)
  {
    return create(headers, destination -> {
      try (InputStream data = blobData) {
        MetricsInputStream input = new MetricsInputStream(data);
        azureClient.create(destination, input);
        return input.getMetrics();
      }
    }, blobId);
  }

  private Blob create(final Map<String, String> headers,
                      final BlobIngester ingester,
                      @Nullable final BlobId assignedBlobId)
  {
    final BlobId blobId = getBlobId(headers, assignedBlobId);

    final String blobPath = contentPath(blobId);
    final String attributePath = attributePath(blobId);
    final boolean isDirectPath = Boolean.parseBoolean(headers.getOrDefault(DIRECT_PATH_BLOB_HEADER, "false"));
    Long existingSize = null;
    if (isDirectPath) {
      AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, attributePath);
      if (exists(blobId)) {
        existingSize = getContentSizeForDeletion(blobAttributes);
      }
    }

    final AzureBlob blob = liveBlobs.getUnchecked(blobId);

    Lock lock = blob.lock();
    try {
      log.debug("Writing blob {} to {}", blobId, blobPath);

      final StreamMetrics streamMetrics = ingester.ingestTo(blobPath);
      final BlobMetrics metrics = new BlobMetrics(new DateTime(), streamMetrics.getSha1(), streamMetrics.getSize());
      blob.refresh(headers, metrics);

      AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, attributePath, headers, metrics);

      blobAttributes.store();
      if (isDirectPath && existingSize != null) {
        storeMetrics.recordDeletion(existingSize);
      }
      storeMetrics.recordAddition(blobAttributes.getMetrics().getContentSize());

      return blob;
    }
    catch (IOException e) {
      // Something went wrong, clean up the files we created
      azureClient.delete(attributePath);
      azureClient.delete(blobPath);
      throw new BlobStoreException(e, blobId);
    }
    finally {
      lock.unlock();
    }
  }

  @Nullable
  private Long getContentSizeForDeletion(final AzureBlobAttributes blobAttributes) {
    try {
      blobAttributes.load();
      return blobAttributes.getMetrics() != null ? blobAttributes.getMetrics().getContentSize() : null;
    }
    catch (Exception e) {
      log.warn("Unable to load attributes {}, delete will not be added to metrics.", blobAttributes, e);
      return null;
    }
  }

  @Override
  @Guarded(by = STARTED)
  public Blob copy(final BlobId blobId, final Map<String, String> headers) {
    Blob sourceBlob = checkNotNull(get(blobId));
    String sourcePath = contentPath(sourceBlob.getId());
    return create(headers, destination -> {
      azureClient.copy(sourcePath, destination);
      BlobMetrics metrics = sourceBlob.getMetrics();
      return new StreamMetrics(metrics.getContentSize(), metrics.getSha1Hash());
    }, null);
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

    if (blob.isStale()) {
      Lock lock = blob.lock();
      try {
        if (blob.isStale()) {
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
  protected boolean doDelete(final BlobId blobId, final String reason) {
    final AzureBlob blob = liveBlobs.getUnchecked(blobId);

    Lock lock = blob.lock();
    try {
      log.debug("Soft deleting blob {}", blobId);

      AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, attributePath(blobId));

      boolean loaded = blobAttributes.load();
      if (!loaded) {
        // This could happen under some concurrent situations (two threads try to delete the same blob)
        // but it can also occur if the deleted index refers to a manually-deleted blob.
        log.warn("Attempt to mark-for-delete non-existent blob {}", blobId);
        return false;
      }
      else if (blobAttributes.isDeleted()) {
        log.debug("Attempt to delete already-deleted blob {}", blobId);
        return false;
      }

      blobAttributes.setDeleted(true);
      blobAttributes.setDeletedReason(reason);
      blobAttributes.store();

      //TODO: Soft delete the blobs some how.
      // Account level - https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-soft-delete
      // Rename?
      // Store in an Azure datastore?
      // Something else?
      blob.markStale();

      return true;
    }
    catch (Exception e) {
      throw new BlobStoreException(e, blobId);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean doDeleteHard(final BlobId blobId) {
    try {
      log.debug("Hard deleting blob {}", blobId);

      String attributePath = attributePath(blobId);
      AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, attributePath);
      Long contentSize = getContentSizeForDeletion(blobAttributes);

      String blobPath = contentPath(blobId);

      azureClient.delete(blobPath);
      azureClient.delete(attributePath);

      if (contentSize != null) {
        storeMetrics.recordDeletion(contentSize);
      }

      return true;
    }
    catch (Exception e) {
      throw new BlobStoreException(e, blobId);
    }
    finally {
      liveBlobs.invalidate(blobId);
    }
  }

  @Override
  @Guarded(by = STARTED)
  public BlobStoreMetrics getMetrics() {
    return storeMetrics.getMetrics();
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
  protected BlobAttributes getBlobAttributes(final AttributesLocation attributesFilePath) throws IOException {
    AzureBlobAttributes azureBlobAttributes = new AzureBlobAttributes(azureClient, attributesFilePath.getFullPath());
    azureBlobAttributes.load();
    return azureBlobAttributes;
  }

  @Override
  protected void doInit(final BlobStoreConfiguration blobStoreConfiguration) {
    try {
      azureClient = azureStorageClientFactory.create(blobStoreConfiguration);
      if (!azureClient.containerExists()) {
        azureClient.createContainer();
      }
    }
    catch (MalformedURLException | InvalidKeyException e) {
      throw new BlobStoreException("Unable to initialize blob store container", e, null);
    }
  }

  @Override
  @Guarded(by = {NEW, STOPPED, FAILED})
  public void remove() {
    // TODO delete bucket only if it is empty
    azureClient.deleteContainer();
  }

  @Override
  @Guarded(by = STARTED)
  public Stream<BlobId> getBlobIdStream() {
    Predicate<String> blobItemPredicate = name -> name.endsWith(BLOB_ATTRIBUTE_SUFFIX);
    return toStream(azureClient.listFiles(CONTENT_PREFIX, blobItemPredicate))
        .map(AzureAttributesLocation::new)
        .map(this::getBlobIdFromAttributeFilePath)
        .map(BlobId::new);
  }

  private Stream<String> toStream(final Observable<String> map) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(map.blockingIterable().iterator(), Spliterator.ORDERED),
        false);
  }

  @Override
  @Guarded(by = STARTED)
  public Stream<BlobId> getDirectPathBlobIdStream(final String prefix) {
    Predicate<String> blobItemPredicate = name -> name.endsWith(BLOB_ATTRIBUTE_SUFFIX);
    return toStream(azureClient.listBlobs(DIRECT_PATH_PREFIX, blobItemPredicate))
        .map(this::attributePathToDirectPathBlobId);
  }

  /**
   * @return the {@link BlobAttributes} for the blob, or null
   * @throws BlobStoreException if an {@link IOException} occurs
   */
  @Override
  @Guarded(by = STARTED)
  public BlobAttributes getBlobAttributes(final BlobId blobId) {
    try {
      AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, attributePath(blobId));
      return blobAttributes.load() ? blobAttributes : null;
    }
    catch (IOException e) {
      log.error("Unable to load AzureBlobAttributes for blob id: {}", blobId, e);
      return null;
    }
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
    AzureBlobAttributes blobAttributes = new AzureBlobAttributes(azureClient, attributePath(blobId));
    try {
      return blobAttributes.load();
    } catch (IOException ioe) {
      log.debug("Unable to load attributes {} during existence check, exception: {}", blobAttributes, ioe);
      return false;
    }
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
  public boolean isStorageAvailable() {
    return false;
  }

  @Override
  protected String attributePathString(final BlobId blobId) {
    return attributePath(blobId);
  }

  @Override
  @Guarded(by = STARTED)
  public boolean isWritable() {
    //TODO: verify container exists?
    return true;
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

  /**
   * Used by {@link #getDirectPathBlobIdStream(String)} to convert an azure key to a {@link BlobId}.
   *
   * @see BlobIdLocationResolver
   */
  private BlobId attributePathToDirectPathBlobId(final String key) { // NOSONAR
    checkArgument(key.startsWith(DIRECT_PATH_PREFIX + "/"), "Not direct path blob path: %s", key);
    checkArgument(key.endsWith(BLOB_ATTRIBUTE_SUFFIX), "Not blob attribute path: %s", key);
    String blobName = key
        .substring(0, key.length() - BLOB_ATTRIBUTE_SUFFIX.length())
        .substring(DIRECT_PATH_PREFIX.length() + 1);
    Map<String, String> headers = ImmutableMap.of(
        BLOB_NAME_HEADER, blobName,
        DIRECT_PATH_BLOB_HEADER, "true"
    );
    return blobIdLocationResolver.fromHeaders(headers);
  }

  class AzureBlob
      extends BlobSupport
  {
    public AzureBlob(final BlobId blobId) {
      super(blobId);
    }

    @Override
    public InputStream getInputStream() {
      try {
        return azureClient.get(contentPath(getId()));
      }
      catch (IOException e) {
        throw new BlobStoreException("caught IOException on client#get", e, getId());
      }
    }
  }

  class AzureAttributesLocation implements AttributesLocation {

    private String key;

    public AzureAttributesLocation(String key) {
      this.key = checkNotNull(key);
    }

    @Override
    public String getFileName() {
      return key.substring(key.lastIndexOf('/') + 1);
    }

    @Override
    public String getFullPath() {
      return key;
    }
  }

  private interface BlobIngester
  {
    StreamMetrics ingestTo(final String destination) throws IOException;
  }
}
