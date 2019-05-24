package org.sonatype.nexus.blobstore.azure.internal.db;

import org.sonatype.nexus.common.entity.AbstractEntity;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple entity class for blob ID
 */
class BlobIdEntity
    extends AbstractEntity
{
  private String blobId;

  String getBlobId() {
    return blobId;
  }

  void setBlobId(final String blobId) {
    this.blobId = checkNotNull(blobId);
  }
}
