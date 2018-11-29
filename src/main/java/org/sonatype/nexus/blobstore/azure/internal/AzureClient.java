package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.sonatype.nexus.utils.ByteBufferInputStream;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.DownloadResponse;
import com.microsoft.azure.storage.blob.StorageException;
import io.reactivex.Flowable;
import org.apache.commons.io.IOUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.rest.v2.util.FlowableUtil.collectBytesInBuffer;

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
}