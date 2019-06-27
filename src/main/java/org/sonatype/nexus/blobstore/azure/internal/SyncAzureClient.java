package org.sonatype.nexus.blobstore.azure.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SyncAzureClient
    implements AzureClient
{
  private final CloudBlobClient serviceClient;

  private final int chunkSize;

  private final String containerName;

  public SyncAzureClient(final CloudBlobClient serviceClient, final int chunkSize, final String containerName) {
    this.serviceClient = checkNotNull(serviceClient);
    this.containerName = checkNotNull(containerName);
    checkArgument(chunkSize > 0, "Chunk size must be > 0");
    this.chunkSize = chunkSize;
  }

  @Override
  public void create(final String path, final InputStream data) {
    try {
      CloudBlockBlob blob = getCloudBlobContainer().getBlockBlobReference(path);

      List<BlockEntry> blockList = new ArrayList<>();
      byte[] buffer = new byte[chunkSize];
      while ((data.available() > 0)) {
        String base64BlockId = createBase64BlockId();
        blockList.add(new BlockEntry(base64BlockId));
        int read = data.read(buffer);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer, 0, read);
        blob.uploadBlock(base64BlockId, byteArrayInputStream, read);
      }
      blob.commitBlockList(blockList);
    }
    catch (URISyntaxException | StorageException | IOException e) {
      throw new RuntimeException(e);
    }
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
    try {
      CloudBlockBlob blob = getCloudBlobContainer().getBlockBlobReference(path);
      PipedInputStream in = new PipedInputStream();
      final PipedOutputStream out = new PipedOutputStream(in);
      blob.download(out);
      out.close();
      return in;
    }
    catch (StorageException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(final String path) {
    try {
      CloudBlockBlob blob = getCloudBlobContainer().getBlockBlobReference(path);
      return blob.exists();
    }
    catch (URISyntaxException | StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(final String path) {
    try {
      CloudBlockBlob blob = getCloudBlobContainer().getBlockBlobReference(path);
      blob.delete();
    }
    catch (URISyntaxException | StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void copy(final String sourcePath, final String destination) {
    try {
      CloudBlockBlob src = getCloudBlobContainer().getBlockBlobReference(sourcePath);
      CloudBlockBlob dest = getCloudBlobContainer().getBlockBlobReference(destination);
      dest.startCopy(src);
      // TODO: Do we need to block until copy is complete?
    }
    catch (URISyntaxException | StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<String> listFiles(final String contentPrefix) {
    return StreamSupport.stream(getCloudBlobContainer().listBlobs(contentPrefix, true).spliterator(), false)
        .map(o -> (CloudBlockBlob) o)
        .map(CloudBlob::getName);
  }

  @Override
  public Stream<String> listFiles(final String contentPrefix, final Predicate<String> blobSuffixFilter) {
    return listFiles(contentPrefix)
        .filter(blobSuffixFilter);
  }

  @Override
  public void createContainer() {
    CloudBlobContainer container = getCloudBlobContainer();
    try {
      container.createIfNotExists();
    }
    catch (StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteContainer() {
    CloudBlobContainer container = getCloudBlobContainer();
    try {
      container.deleteIfExists();
    }
    catch (StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean containerExists() {
    CloudBlobContainer container = getCloudBlobContainer();
    try {
      return container.exists();
    }
    catch (StorageException e) {
      throw new RuntimeException(e);
    }
  }

  private CloudBlobContainer getCloudBlobContainer() {
    try {
      return serviceClient.getContainerReference(getContainerName());
    }
    catch (URISyntaxException | StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getContainerName() {
    return containerName;
  }
}
