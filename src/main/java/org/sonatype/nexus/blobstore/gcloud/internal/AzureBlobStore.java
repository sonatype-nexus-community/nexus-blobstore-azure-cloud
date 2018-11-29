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
package org.sonatype.nexus.blobstore.gcloud.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.nexus.blobstore.BlobIdLocationResolver;
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
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;

import com.google.common.hash.HashCode;

import static com.google.common.base.Preconditions.checkNotNull;
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
    extends StateGuardLifecycleSupport
    implements BlobStore
{
  public static final String TYPE = "Azure Cloud Storage";

  public static final String CONFIG_KEY = TYPE.toLowerCase();

  public static final String BUCKET_KEY = "bucket";

  public static final String CREDENTIAL_FILE_KEY = "credential_file";

  public static final String BLOB_CONTENT_SUFFIX = ".bytes";

  public static final String BLOB_ATTRIBUTE_SUFFIX = ".properties";

  static final String CONTENT_PREFIX = "content";

  private final BlobIdLocationResolver blobIdLocationResolver;

  private final AzureBlobStoreMetricsStore metricsStore;

  private BlobStoreConfiguration blobStoreConfiguration;

  private final DryRunPrefix dryRunPrefix;

  @Inject
  public AzureBlobStore(final BlobIdLocationResolver blobIdLocationResolver,
                        final AzureBlobStoreMetricsStore metricsStore,
                        final DryRunPrefix dryRunPrefix) {
    this.blobIdLocationResolver = checkNotNull(blobIdLocationResolver);
    this.metricsStore = metricsStore;
    this.dryRunPrefix = dryRunPrefix;
  }

  @Override
  protected void doStart() throws Exception {
    metricsStore.start();
  }

  @Override
  protected void doStop() throws Exception {
    metricsStore.stop();
  }

  @Override
  @Guarded(by = STARTED)
  public Blob create(final InputStream inputStream, final Map<String, String> headers) {
    checkNotNull(inputStream);

    return null;
  }

  @Override
  @Guarded(by = STARTED)
  public Blob create(final Path path, final Map<String, String> map, final long size, final HashCode hash) {
    throw new BlobStoreException("hard links not supported", null);
  }

  @Override
  @Guarded(by = STARTED)
  public Blob create(final InputStream inputStream, final Map<String,String> headers, BlobId blobId) {
    throw new UnsupportedOperationException("fixme");
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

    return null;
  }



  @Override
  @Guarded(by = STARTED)
  public boolean delete(final BlobId blobId, final String reason) {
    checkNotNull(blobId);

    return false;
  }

  @Override
  @Guarded(by = STARTED)
  public boolean deleteHard(final BlobId blobId) {
    checkNotNull(blobId);
    return false;
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
  public void init(final BlobStoreConfiguration blobStoreConfiguration) throws Exception {

  }

  @Override
  @Guarded(by = {NEW, STOPPED, FAILED})
  public void remove() {
    // TODO delete bucket only if it is empty
  }

  @Override
  @Guarded(by = STARTED)
  public Stream<BlobId> getBlobIdStream() {
    return null;
  }

  @Override
  @Guarded(by = STARTED)
  public Stream<BlobId> getDirectPathBlobIdStream(final String prefix) {
    return null;
  }

  /**
   * @return the {@link BlobAttributes} for the blob, or null
   * @throws BlobStoreException if an {@link IOException} occurs
   */
  @Override
  @Guarded(by = STARTED)
  public BlobAttributes getBlobAttributes(final BlobId blobId) {
    return null;
  }

  @Override
  @Guarded(by = STARTED)
  public void setBlobAttributes(final BlobId blobId, final BlobAttributes blobAttributes) {
    AzureBlobAttributes existing = (AzureBlobAttributes) getBlobAttributes(blobId);
    if (existing != null) {
      try {
        existing.updateFrom(blobAttributes);
        existing.store();
      } catch (IOException e) {
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

  private String getConfiguredBucketName() {
    return blobStoreConfiguration.attributes(CONFIG_KEY).require(BUCKET_KEY).toString();
  }

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
}
