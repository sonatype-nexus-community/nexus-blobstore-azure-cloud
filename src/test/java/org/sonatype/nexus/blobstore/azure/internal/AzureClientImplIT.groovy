package org.sonatype.nexus.blobstore.azure.internal

import java.nio.charset.Charset

import org.sonatype.nexus.blobstore.MockBlobStoreConfiguration

import org.apache.commons.io.IOUtils
import spock.lang.Specification

import static java.util.stream.Collectors.toList

class AzureClientImplIT
    extends Specification
{
  private AzureClient client

  def setup() {
    def configuration = new MockBlobStoreConfiguration(attributes: [
        (AzureBlobStore.CONFIG_KEY): [
            (AzureBlobStore.ACCOUNT_NAME_KEY)  : System.getProperty('nxrm.azure.accountName'),
            (AzureBlobStore.ACCOUNT_KEY_KEY)   : System.getProperty('nxrm.azure.accountKey'),
            (AzureBlobStore.CONTAINER_NAME_KEY): UUID.randomUUID().toString(),
        ]
    ])
    client = new AzureStorageClientFactory(10000, 30, 30).create(configuration)
    assert client instanceof AzureClientImpl
    this.client.createContainer()
  }

  def cleanup() {
    client.deleteContainer()
  }

  def "It will create and get a file"() {
    given: 'A blob'
      def blobName = 'testBlob'
      def blobPath = "${blobName}.properties"
      String data = 'Hello world!' * 20

    when: 'The blob is created with the client'
      client.create(blobPath, new ByteArrayInputStream(data.getBytes()))

    then: 'The client reports the blob exists'
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
    given: 'some blobs'
      assert !client.listFiles('path/').findAny().isPresent()
      client.create('file1.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('file2.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('path/file1.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('path/file2.txt', new ByteArrayInputStream('helloworld'.bytes))
    when:
      def files = client.listFiles('', { x -> x.endsWith('.txt') }).collect(toList())
    then: 'all to be returned with no prefix'
      files == ['file1.txt', 'file2.txt', 'path/file1.txt', 'path/file2.txt']
    when:
      files = client.listFiles('path/', { x -> x.endsWith('.txt') }).collect(toList())
    then:
      files == ['path/file1.txt', 'path/file2.txt']
  }
}
