package org.sonatype.nexus.blobstore.azure.internal;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.InvalidKeyException;

import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.goodies.common.ComponentSupport;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;

import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.Slf4jLogger;

import static com.microsoft.azure.storage.blob.StorageURL.createPipeline;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.ACCOUNT_KEY_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.ACCOUNT_NAME_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CONFIG_KEY;
import static org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.CONTAINER_NAME_KEY;

/**
 *
 */
@Named
public class AzureStorageClientFactory
    extends ComponentSupport
{
  private int chunkSize;

  @Inject
  public AzureStorageClientFactory(@Named("${nexus.azure.blocksize:-5242880}") final int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public AzureClient create(final BlobStoreConfiguration blobStoreConfiguration)
      throws MalformedURLException, InvalidKeyException
  {

    String accountName = blobStoreConfiguration.attributes(CONFIG_KEY).get(ACCOUNT_NAME_KEY, String.class);
    String accountKey = blobStoreConfiguration.attributes(CONFIG_KEY).get(ACCOUNT_KEY_KEY, String.class);
    String containerName = blobStoreConfiguration.attributes(CONFIG_KEY).get(CONTAINER_NAME_KEY, String.class);

    SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);
    // TODO: This doesn't seem to help reduce logging spam...
    PipelineOptions pipelineOptions = new PipelineOptions()
        .withLogger(new Slf4jLogger(log).withMinimumLogLevel(HttpPipelineLogLevel.OFF));
    HttpPipeline pipeline = createPipeline(credential, pipelineOptions);
    URL u = new URL(format(ROOT, "https://%s.blob.core.windows.net", accountName));
    ServiceURL serviceURL = new ServiceURL(u, pipeline);
    ContainerURL containerURL = serviceURL.createContainerURL(containerName);
    return new AzureClient(containerURL, chunkSize);
  }
}
