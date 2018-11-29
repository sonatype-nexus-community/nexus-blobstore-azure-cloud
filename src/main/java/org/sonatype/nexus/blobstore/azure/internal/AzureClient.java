package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import org.sonatype.nexus.utils.ByteBufferInputStream;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.DownloadResponse;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.StorageException;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import org.apache.commons.io.IOUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.rest.v2.util.FlowableUtil.collectBytesInBuffer;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.BLOB_ATTRIBUTE_SUFFIX;

public class AzureClient
{

  private final ContainerURL containerURL;

  public AzureClient(final ContainerURL containerURL)
  {
    this.containerURL = checkNotNull(containerURL);
  }

  public DownloadResponse create(final String path,
                                 final InputStream data) throws IOException
  {
    byte[] bytes = IOUtils.toByteArray(data);
    Flowable<ByteBuffer> just = Flowable.just(ByteBuffer.wrap(bytes));
    BlockBlobURL blobURL = containerURL.createBlockBlobURL(path);
    DownloadResponse downloadResponse = blobURL.upload(just, bytes.length, null, null, null, null)
        .flatMap(blobsDownloadResponse ->
            // Download the blob's content.
            blobURL.download(null, null, false, null))
        .blockingGet();
    return downloadResponse;
  }

  public InputStream get(final String path) {
    ByteBuffer byteBuffer = containerURL.createBlockBlobURL(path)
        .download()
        .flatMap(blobsDownloadResponse -> collectBytesInBuffer(blobsDownloadResponse.body(null)))
        .blockingGet();
    return new ByteBufferInputStream(byteBuffer);
  }

  public boolean exists(final String path) {
    try {
      containerURL.createBlobURL(path).getProperties()
          .blockingGet();
      return true;
    }
    catch (StorageException e) {
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

  public Stream<String> listBlobs(final String contentPrefix) {
    Builder<String> builder = Stream.builder();

    ListBlobsOptions listBlobsOptions = new ListBlobsOptions().withPrefix(contentPrefix).withMaxResults(5);
    Observable<BlobItem> itemObservable = containerURL
        .listBlobsFlatSegment(null, listBlobsOptions)
        .flatMapObservable(r -> listContainersResultToContainerObservable(containerURL, listBlobsOptions, r));
    Observable<String> map = itemObservable
        .filter(blobItem -> blobItem.name().endsWith(BLOB_ATTRIBUTE_SUFFIX))
        .map(blobItem -> blobItem.name().substring(0, blobItem.name().length() - BLOB_ATTRIBUTE_SUFFIX.length()));

    map.subscribe(builder::add);

    return builder.build();
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
}