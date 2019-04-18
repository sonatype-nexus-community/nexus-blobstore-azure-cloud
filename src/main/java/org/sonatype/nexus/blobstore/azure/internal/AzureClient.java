package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.UUID;

import javax.annotation.Nullable;

import org.sonatype.goodies.common.ComponentSupport;

import com.github.davidmoten.rx2.Bytes;
import com.google.common.io.ByteSource;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.StorageException;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.BlockBlobCommitBlockListResponse;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.functions.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.rest.v2.util.FlowableUtil.collectBytesInBuffer;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.BLOB_ATTRIBUTE_SUFFIX;

public class AzureClient
    extends ComponentSupport
{

  private final ContainerURL containerURL;

  private int chunkSize;

  public AzureClient(final ContainerURL containerURL, final int chunkSize) {
    this.containerURL = checkNotNull(containerURL);
    checkArgument(chunkSize > 0, "Chunk size must be > 0");
    this.chunkSize = chunkSize;
  }

  public BlockBlobCommitBlockListResponse create(final String path, final InputStream data) {
    ArrayList<String> blockIds = new ArrayList<>();
    BlockBlobURL blobURL = containerURL.createBlockBlobURL(path);

    return Bytes.from(data, chunkSize).toObservable().concatMapEager(buffer -> {
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

  public InputStream get(final String path) throws IOException {
    ByteBuffer byteBuffer = containerURL.createBlockBlobURL(path)
        .download()
        .flatMap(blobsDownloadResponse -> collectBytesInBuffer(blobsDownloadResponse.body(null)))
        .blockingGet();

    return ByteSource.wrap(byteBuffer.array()).openStream();
    //return new ByteBufferInputStream(byteBuffer);
  }

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

  public void delete(final String path) {
    containerURL.createBlockBlobURL(path).delete().blockingGet();
  }

  public void copy(final String sourcePath, final String destination) {
    BlockBlobURL sourceBlob = containerURL.createBlockBlobURL(sourcePath);
    containerURL.createBlockBlobURL(destination).startCopyFromURL(sourceBlob.toURL()).blockingGet();
  }

  public Observable<String> listBlobs(final String contentPrefix, @Nullable final Predicate<String> blobSuffixFilter) {
    Predicate<String> filter = b -> true;
    filter = blobSuffixFilter != null ? blobSuffixFilter : filter;

    ListBlobsOptions listBlobsOptions = new ListBlobsOptions().withPrefix(contentPrefix).withMaxResults(5);
    Observable<BlobItem> itemObservable = containerURL
        .listBlobsFlatSegment(null, listBlobsOptions)
        .flatMapObservable(r -> listContainersResultToContainerObservable(containerURL, listBlobsOptions, r));
    return itemObservable
        .map(BlobItem::name)
        .filter(filter)
        .map(key -> key.substring(key.lastIndexOf('/') + 1))
        .map(fileName -> fileName.substring(0, fileName.length() - BLOB_ATTRIBUTE_SUFFIX.length()));
  }

  public Observable<String> listFiles(final String contentPrefix, final Predicate<String> blobSuffixFilter) {
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions().withPrefix(contentPrefix).withMaxResults(5);
    Observable<BlobItem> itemObservable = containerURL
        .listBlobsFlatSegment(null, listBlobsOptions)
        .flatMapObservable(r -> listContainersResultToContainerObservable(containerURL, listBlobsOptions, r));
    return itemObservable
        .map(BlobItem::name)
        .filter(blobSuffixFilter);
  }

  private static Observable<BlobItem> listContainersResultToContainerObservable(
      ContainerURL containerURL, ListBlobsOptions listBlobsOptions,
      ContainerListBlobFlatSegmentResponse response)
  {
    Observable<BlobItem> result = Observable.fromIterable(response.body().segment().blobItems());

    System.out.println("!!! count: " + response.body().segment().blobItems());

    if (response.body().nextMarker() != null) {
      System.out.println("Hit continuation in listing at " + response.body().segment().blobItems().get(
          response.body().segment().blobItems().size() - 1).name());
      // Recursively add the continuation items to the observable.
      result = result.concatWith(containerURL.listBlobsFlatSegment(response.body().nextMarker(), listBlobsOptions,
          null)
          .flatMapObservable((r) ->
              listContainersResultToContainerObservable(containerURL, listBlobsOptions, r)));
    }

    return result;
  }

  public void createContainer() {
    containerURL.create().blockingGet();
  }

  public void deleteContainer() {
    containerURL.delete().blockingGet();
  }

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
}