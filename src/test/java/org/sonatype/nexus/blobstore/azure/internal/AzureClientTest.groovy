package org.sonatype.nexus.blobstore.azure.internal

import java.nio.charset.Charset

import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration

import org.apache.commons.io.IOUtils
import spock.lang.Specification

class AzureClientTest
    extends Specification
{
  def "It will create and get a file"() {
    given: 'A client'
      def configuration = new BlobStoreConfiguration(attributes: [
          (AzureBlobStore.CONFIG_KEY): [
              (AzureBlobStore.ACCOUNT_NAME_KEY)  : System.getProperty('nxrm.azure.accountName'),
              (AzureBlobStore.ACCOUNT_KEY_KEY)   : System.getProperty('nxrm.azure.accountKey'),
              (AzureBlobStore.CONTAINER_NAME_KEY): System.getProperty('nxrm.azure.containerName'),
          ]
      ])
      def client = new AzureStorageClientFactory().create(configuration)

    and: 'A blob'
      def blobName = 'testBlob'
      String data = 'Hello world!'

    when: 'The blob is created with the client'
      def downloadResponse = client.create(blobName, new ByteArrayInputStream(data.getBytes()))

    then: 'The response is downloaded'
      downloadResponse.statusCode() == 206

    and: 'The client reports the blob exists'
      client.exists(blobName)

    when: 'The blob is retrieved'
      def is = client.get(blobName)

    then: 'The blob matches the expected'
      def actual = IOUtils.toString(is, Charset.defaultCharset())
      actual == data

    when: 'The blob is deleted'
      client.delete(blobName)

    then: 'The blob no longer exists'
      !client.exists(blobName)
  }
}
