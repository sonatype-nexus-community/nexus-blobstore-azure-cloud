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
package org.sonatype.nexus.blobstore.azure.internal

import java.nio.charset.Charset

import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration

import org.apache.commons.io.IOUtils
import spock.lang.Specification

class AzureClientIT
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
    client = new AzureStorageClientFactory(1000).create(configuration)
    this.client.createContainer()
  }

  def cleanup() {
    client.deleteContainer()
  }

  def "It will create and get a file"() {
    given: 'A blob'
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
      assert client.listFiles('path/').isEmpty().blockingGet()
      client.create('file1.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('file2.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('path/file1.txt', new ByteArrayInputStream('helloworld'.bytes))
      client.create('path/file2.txt', new ByteArrayInputStream('helloworld'.bytes))
    when:
      def files = client.listFiles('', { x -> x.endsWith('.txt') })
    then: 'all to be returned with no prefix'
      files.blockingIterable().iterator().collect() == ['file1.txt', 'file2.txt', 'path/file1.txt', 'path/file2.txt']
    when:
      files = client.listFiles('path/', { x -> x.endsWith('.txt') })
    then:
      files.blockingIterable().iterator().collect() == ['path/file1.txt', 'path/file2.txt']
  }
}
