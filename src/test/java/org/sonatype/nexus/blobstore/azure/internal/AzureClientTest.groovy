package org.sonatype.nexus.blobstore.azure.internal

import java.nio.charset.Charset

import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration

import io.reactivex.Observable
import org.apache.commons.io.IOUtils
import spock.lang.Specification

class AzureClientTest
    extends Specification
{
  private AzureClient client

  def setup() {
    def configuration = new BlobStoreConfiguration(attributes: [
        (AzureBlobStore.CONFIG_KEY): [
            (AzureBlobStore.ACCOUNT_NAME_KEY)  : System.getProperty('nxrm.azure.accountName'),
            (AzureBlobStore.ACCOUNT_KEY_KEY)   : System.getProperty('nxrm.azure.accountKey'),
            (AzureBlobStore.CONTAINER_NAME_KEY): UUID.randomUUID().toString(),
        ]
    ])
    client = new AzureStorageClientFactory(2).create(configuration)
    this.client.createContainer()
  }

  def cleanup() {
    client.deleteContainer()
  }

  def "It will create and get a file"() {
    given: 'A client'


    and: 'A blob'
      def blobName = 'testBlob'
      def blobPath = "${blobName}.properties"
      String data = 'Hello world!' * 200

    when: 'The blob is created with the client'
      def downloadResponse = client.create(blobPath, new ByteArrayInputStream(data.getBytes()))

    then: 'The response is downloaded'
      downloadResponse.statusCode() == 201

    and: 'The client reports the blob exists'
      client.exists(blobPath)

    when: 'The blob is retrieved'
      def is = client.get(blobPath)

    then: 'The blob matches the expected'
      def actual = IOUtils.toString(is, Charset.defaultCharset())
      actual == data

    when: 'The blob is copied'
      def blobNamePathCopy = "${blobName}_copy.properties"
      client.copy(blobPath, blobNamePathCopy)

    then: 'The copy exists'
      client.exists(blobNamePathCopy)

    and: 'The client can list the files'
      Observable<String> files = client.listBlobs('',
          { blobItem -> blobItem.endsWith(AzureBlobStore.BLOB_ATTRIBUTE_SUFFIX) })
      def results = []
      files.blockingForEach({ results.add(it) })
      results == ['testBlob', 'testBlob_copy']

    when: 'The blob and its copy are deleted'
      client.delete(blobPath)
      client.delete(blobNamePathCopy)

    then: 'The blobs no longer exists'
      !client.exists(blobPath)
      !client.exists(blobNamePathCopy)
  }

  def "It will detect if the container exists"() {
    when: 'the container exists'
      client.containerExists()

    and: 'the container is delete'
      client.deleteContainer()

    then: 'the container no longer exists'
      !client.containerExists()
  }

  def "It will list files"() {
    expect:
      client.create('file1.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('file2.txt', new ByteArrayInputStream('helloworld'.bytes))
      def files = client.listFiles('', { x -> x.endsWith('.txt') })
      files.blockingIterable().iterator().collect() == ['file1.txt', 'file2.txt']
  }
}
