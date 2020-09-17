package org.sonatype.nexus.blobstore.azure.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.sonatype.goodies.common.ComponentSupport;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobBeginCopyOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AzureClientImpl
    extends ComponentSupport
    implements AzureClient
{
  private final BlobServiceClient serviceClient;

  private final int chunkSize;

  private final String containerName;

  private final int copyTimeout;

  private final int listBlobsTimeout;

  public AzureClientImpl(final BlobServiceClient serviceClient,
                         final String containerName,
                         final int chunkSize,
                         final int copyTimeout,
                         final int listBlobsTimeout) {
    this.serviceClient = checkNotNull(serviceClient);
    this.containerName = checkNotNull(containerName);
    checkArgument(chunkSize > 0, "Chunk size must be > 0");
    this.chunkSize = chunkSize;
    checkArgument(copyTimeout > 0, "Copy timout must be > 0");
    this.copyTimeout = copyTimeout;
    checkArgument(listBlobsTimeout > 0, "List blob timeout must be > 0");
    this.listBlobsTimeout = listBlobsTimeout;
  }

  @Override
  public void create(final String path, final InputStream data) {
    log.debug("Creating blob {}", path);
    try {
      BlockBlobClient blockBlobClient = getCloudBlobContainer().getBlobClient(path)
          .getBlockBlobClient();
      List<String> blockList = new ArrayList<>();
      int totalRead = 0;
      int bytesRead;
      byte[] buffer = new byte[chunkSize];
      while ((bytesRead = data.read(buffer, totalRead, buffer.length - totalRead)) != -1) {
        totalRead += bytesRead;
        if (totalRead == buffer.length) { // entire buffer was filled so upload block
          blockList.add(uploadBlock(blockBlobClient, totalRead, buffer));
          totalRead = 0; // reset totalRead so buffer will be reused
        }
      }
      if (totalRead > 0) { // write the last block if it exists
        blockList.add(uploadBlock(blockBlobClient, totalRead, buffer));
      }
      log.debug("Blocks committed for {} -> {}", path, blockList.size());
      blockBlobClient.commitBlockList(blockList, true);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String uploadBlock(final BlockBlobClient blob, final int length, final byte[] data)
  {
    String base64BlockId = createBase64BlockId();
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, 0, length);
    blob.stageBlock(base64BlockId, byteArrayInputStream, length);
    return base64BlockId;
  }

  private static String createBase64BlockId() {
    UUID uuid = UUID.randomUUID();
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return Base64.getEncoder().encodeToString(bb.array());
  }

  @Override
  public InputStream get(final String path) {
    log.debug("Getting blob {}", path);
    return getCloudBlobContainer().getBlobClient(path).openInputStream();
  }

  @Override
  public boolean exists(final String path) {
    boolean exists = getCloudBlobContainer().getBlobClient(path).exists();
    log.debug("{} exists? -> {}", path, exists);
    return exists;
  }

  @Override
  public void delete(final String path) {
    log.debug("Deleting blob {}", path);
    getCloudBlobContainer().getBlobClient(path).delete();
  }

  @Override
  public void copy(final String sourcePath, final String destination) {
    log.debug("Copying blob {} => {}", sourcePath, destination);
    BlobClient srcClient = getCloudBlobContainer().getBlobClient(sourcePath);
    BlobClient destClient = getCloudBlobContainer().getBlobClient(destination);
    SyncPoller<BlobCopyInfo, Void> poller = destClient
        .beginCopy(new BlobBeginCopyOptions(srcClient.getBlobUrl()));
    LongRunningOperationStatus status = poller.waitForCompletion(Duration.ofSeconds(copyTimeout)).getStatus();
    if (!status.isComplete()) {
      log.warn("Copying blob {} is taking a while and will affect performance", srcClient.getBlobUrl());
    }
  }

  @Override
  public Stream<String> listFiles(final String contentPrefix) {
    ListBlobsOptions options = new ListBlobsOptions();
    options.setPrefix(contentPrefix);
    return getCloudBlobContainer()
        .listBlobs(options, Duration.ofSeconds(listBlobsTimeout))
        .stream().map(BlobItem::getName);
  }

  @Override
  public Stream<String> listFiles(final String contentPrefix, final Predicate<String> blobSuffixFilter) {
    return listFiles(contentPrefix)
        .filter(blobSuffixFilter);
  }

  @Override
  public void createContainer() {
    if (!getCloudBlobContainer().exists()) {
      getCloudBlobContainer().create();
    }
  }

  @Override
  public void deleteContainer() {
    getCloudBlobContainer().delete();
  }

  @Override
  public boolean containerExists() {
    return getCloudBlobContainer().exists();
  }

  private BlobContainerClient getCloudBlobContainer() {
    return serviceClient.getBlobContainerClient(getContainerName());
  }

  @Override
  public String getContainerName() {
    return containerName;
  }
}
