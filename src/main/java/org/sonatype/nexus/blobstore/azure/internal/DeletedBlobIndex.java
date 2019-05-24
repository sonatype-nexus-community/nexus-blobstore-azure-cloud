package org.sonatype.nexus.blobstore.azure.internal;

import java.util.stream.Stream;

import org.sonatype.nexus.blobstore.api.BlobId;

/**
 * Simple contract for tracking soft-deleted blobs
 */
public interface DeletedBlobIndex
{
  void add(final BlobId blobId);

  void remove(final BlobId blobId);

  Stream<BlobId> browse();
}
