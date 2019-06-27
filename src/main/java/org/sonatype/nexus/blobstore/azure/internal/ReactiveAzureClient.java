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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.sonatype.goodies.common.ComponentSupport;

import com.github.davidmoten.rx2.Bytes;
import com.google.common.io.ByteSource;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.StorageException;
import com.microsoft.azure.storage.blob.models.BlobFlatListSegment;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import io.reactivex.Flowable;
import io.reactivex.Observable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.rest.v2.util.FlowableUtil.collectBytesInBuffer;

public class ReactiveAzureClient
    extends ComponentSupport
    implements AzureClient
{
  private final ContainerURL containerURL;

  private final String containerName;

  private int chunkSize;

  public ReactiveAzureClient(final ContainerURL containerURL, final int chunkSize, final String containerName) {
    this.containerURL = checkNotNull(containerURL);
    this.containerName = checkNotNull(containerName);
    checkArgument(chunkSize > 0, "Chunk size must be > 0");
    this.chunkSize = chunkSize;
  }

  @Override
  public void create(final String path, final InputStream data) {
    ArrayList<String> blockIds = new ArrayList<>();
    BlockBlobURL blobURL = containerURL.createBlockBlobURL(path);

    Bytes.from(data, chunkSize).toObservable().concatMapEager(buffer -> {
      final String blockId = createBase64BlockId();
      blockIds.add(blockId);
      Flowable<ByteBuffer> just = Flowable.just(ByteBuffer.wrap(buffer));
      return blobURL.stageBlock(blockId, just, buffer.length)
          .map(x -> blockId)
          .toObservable();
    }, 1, 1)
        .collectInto(new ArrayList<String>(), ArrayList::add)
        .flatMap(ids -> blobURL.commitBlockList(blockIds))
        .blockingGet();
  }

  private static String createBase64BlockId() {
    UUID uuid = UUID.randomUUID();
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return Base64.getEncoder().encodeToString(bb.array());
  }

  @Override
  public InputStream get(final String path) throws IOException {
    ByteBuffer byteBuffer = containerURL.createBlockBlobURL(path)
        .download()
        .flatMap(blobsDownloadResponse -> collectBytesInBuffer(blobsDownloadResponse.body(null)))
        .blockingGet();

    return ByteSource.wrap(byteBuffer.array()).openStream();
    //return new ByteBufferInputStream(byteBuffer);
  }

  @Override
  public boolean exists(final String path) {
    try {
      containerURL.createBlobURL(path).getProperties().blockingGet();
      return true;
    }
    catch (StorageException e) {
      log.debug("Path {} does not exist with reason {}", path, e.message(), e);
      return false;
    }
  }

  @Override
  public void delete(final String path) {
    containerURL.createBlockBlobURL(path).delete().blockingGet();
  }

  @Override
  public void copy(final String sourcePath, final String destination) {
    BlockBlobURL sourceBlob = containerURL.createBlockBlobURL(sourcePath);
    containerURL.createBlockBlobURL(destination).startCopyFromURL(sourceBlob.toURL()).blockingGet();
  }

  @Override
  public Stream<String> listFiles(final String contentPrefix) {
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions().withPrefix(contentPrefix).withMaxResults(5);
    Observable<BlobItem> itemObservable = containerURL
        .listBlobsFlatSegment(null, listBlobsOptions)
        .flatMapObservable(r -> listContainersResultToContainerObservable(containerURL, listBlobsOptions, r));
    return toStream(itemObservable
        .map(BlobItem::name));
  }

  @Override
  public Stream<String> listFiles(final String contentPrefix, final Predicate<String> blobSuffixFilter) {
    return listFiles(contentPrefix)
        .filter(blobSuffixFilter);
  }

  private Stream<String> toStream(final Observable<String> map) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(map.blockingIterable().iterator(), Spliterator.ORDERED),
        false);
  }

  private static Observable<BlobItem> listContainersResultToContainerObservable(
      final ContainerURL containerURL,
      final ListBlobsOptions listBlobsOptions,
      final ContainerListBlobFlatSegmentResponse response)
  {
    BlobFlatListSegment segment = response.body().segment();
    if (segment == null) {
      return Observable.empty();
    }
    Observable<BlobItem> result = Observable.fromIterable(segment.blobItems());
    if (response.body().nextMarker() != null) {
      result = result.concatWith(containerURL.listBlobsFlatSegment(response.body().nextMarker(), listBlobsOptions,
          null)
          .flatMapObservable((r) ->
              listContainersResultToContainerObservable(containerURL, listBlobsOptions, r)));
    }
    return result;
  }

  @Override
  public void createContainer() {
    containerURL.create().blockingGet();
  }

  @Override
  public void deleteContainer() {
    containerURL.delete().blockingGet();
  }

  @Override
  public boolean containerExists() {
    try {
      containerURL.getProperties().blockingGet();
      return true;
    }
    catch (StorageException e) {
      if (e.response().statusCode() == 404) {
        return false;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getContainerName() {
    return containerName;
  }
}