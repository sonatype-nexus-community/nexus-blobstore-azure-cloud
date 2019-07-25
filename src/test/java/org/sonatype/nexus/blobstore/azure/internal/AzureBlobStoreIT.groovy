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

import org.sonatype.nexus.blobstore.BlobIdLocationResolver
import org.sonatype.nexus.blobstore.DefaultBlobIdLocationResolver
import org.sonatype.nexus.blobstore.api.BlobId
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration
import org.sonatype.nexus.blobstore.azure.internal.AzureBlobStore.AzureAttributesLocation
import org.sonatype.nexus.blobstore.azure.internal.db.OrientDeletedBlobEntityAdapter
import org.sonatype.nexus.blobstore.azure.internal.db.OrientDeletedBlobIndex
import org.sonatype.nexus.common.log.DryRunPrefix
import org.sonatype.nexus.orient.testsupport.DatabaseInstanceRule

import org.junit.Rule
import spock.lang.Specification

import static java.util.stream.Collectors.toList
import static org.sonatype.nexus.blobstore.DefaultBlobIdLocationResolver.DIRECT_PATH_BLOB_ID_PREFIX

class AzureBlobStoreIT
    extends Specification
{
  private AzureBlobStore azureBlobStore

  private ReactiveAzureClient azureClient

  @Rule
  public DatabaseInstanceRule database = DatabaseInstanceRule.inMemory("azureTest")

  void setup() {
    BlobStoreConfiguration configuration = new BlobStoreConfiguration(
        name: 'azure',
        type: AzureBlobStore.TYPE,
        attributes: [
            'azure cloud storage': [
                (AzureBlobStore.ACCOUNT_NAME_KEY)  : System.getProperty('nxrm.azure.accountName'),
                (AzureBlobStore.ACCOUNT_KEY_KEY)   : System.getProperty('nxrm.azure.accountKey'),
                (AzureBlobStore.CONTAINER_NAME_KEY): UUID.randomUUID().toString()
            ]
        ]
    )
    BlobIdLocationResolver resolver = new DefaultBlobIdLocationResolver()
    AzureBlobStoreMetricsStore storeMetrics = Mock(AzureBlobStoreMetricsStore)
    DryRunPrefix dryRunPrefix = new DryRunPrefix("dr")
    DeletedBlobIndex deletedBlobIndex = new OrientDeletedBlobIndex(new OrientDeletedBlobEntityAdapter(),
        database.instanceProvider)
    deletedBlobIndex.start()

    def factory = new AzureStorageClientFactory(20)
    azureClient = factory.create(configuration)
    azureBlobStore = new AzureBlobStore(factory, resolver, storeMetrics, dryRunPrefix, deletedBlobIndex)
    this.azureBlobStore.init(configuration)
    this.azureBlobStore.start()
  }

  void cleanup() {
    if (azureClient.containerExists()) {
      azureBlobStore.remove()
    }
  }

  def "direct path blobs ids can be streamed"() {
    expect: 'storage is available and writable'
      azureBlobStore.isStorageAvailable()
      azureBlobStore.isWritable()
    and: 'the direct path blob id stream is empty'
      !azureBlobStore.getDirectPathBlobIdStream('blah').anyMatch({ Objects.nonNull(it) })
    when: 'A new blob is created'
      byte[] bytes = [0, 0, 0, 1] as byte[]
      def headers = [
          'BlobStore.blob-name' : 'foo',
          'BlobStore.created-by': 'foo'
      ]
      def blob = azureBlobStore.
          create(new ByteArrayInputStream(bytes), headers, new BlobId(DIRECT_PATH_BLOB_ID_PREFIX + 'blah/foo'))
    and: 'the blob id is located from the direct path stream'
      def directPathBlobId = azureBlobStore.getDirectPathBlobIdStream('blah')
          .filter({ directPathBlobId -> directPathBlobId == blob.id })
          .findFirst()
    then: 'Its blobId will be found and match the expected format'
      directPathBlobId.get().asUniqueString() == 'path$blah/foo'
    when: 'more direct path blobs are created'
      (1..10).each {
        String name = "foo$it"
        headers = [
            'BlobStore.blob-name' : name,
            'BlobStore.created-by': 'foo'
        ]
        blob = azureBlobStore.
            create(new ByteArrayInputStream(bytes), headers, new BlobId(DIRECT_PATH_BLOB_ID_PREFIX + "blah/$name"))
      }
    then: 'the blob stream will contain all of the blob ids'
      azureBlobStore.getDirectPathBlobIdStream('blah').collect(toList()).size() == 11
  }

  def "It will correctly work with a blob"() {
    expect: 'storage is available and writable'
      azureBlobStore.isStorageAvailable()
      azureBlobStore.isWritable()
    when: 'A new blob is created'
      byte[] bytes = [0, 0, 0, 1] as byte[]
      def headers = [
          'BlobStore.blob-name' : 'foo',
          'BlobStore.created-by': 'foo'
      ]
      def blob = azureBlobStore.create(new ByteArrayInputStream(bytes), headers)
    and: 'the blob id is located from the stream'
      Optional<BlobId> blobId = azureBlobStore.getBlobIdStream()
          .filter({ blobId -> blobId == blob.id })
          .findFirst()
    then: 'Its blobId will be found and match the expected format'
      blobId.get().asUniqueString() ==~ /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/
    and: 'the blob id exists'
      azureBlobStore.exists(blobId.get())
    when: 'the blob is copied'
      def copy = azureBlobStore.copy(blobId.get(), headers)
    then: 'the copy exists'
      azureBlobStore.exists(copy.id)
    when: 'the blob is soft deleted'
      azureBlobStore.delete(blobId.get(), 'for testing')
    then: 'the blob still exists'
      azureBlobStore.exists(blobId.get())
    and: 'the soft deleted blob can be fetched'
      azureBlobStore.get(blobId.get(), true)
    and: 'the blob attributes indicate it was deleted'
      azureBlobStore.getBlobAttributes(blobId.get()).isDeleted()
      azureBlobStore.getBlobAttributes(new AzureAttributesLocation(azureBlobStore.attributePath(blobId.get()))).
          isDeleted()
      azureBlobStore.softDeletes().browse().collect(toList()).contains(blobId.get())
    when: 'the blob store is compacted'
      azureBlobStore.compact()
    then: 'the soft deleted blobs are removed'
      !azureBlobStore.exists(blobId.get())
      !azureBlobStore.softDeletes().browse().collect(toList()).contains(blobId.get())
  }

  def "Missing blobs will not exist"() {
    expect: 'a non sense blob will not exist'
      !azureBlobStore.exists(new BlobId('this_is_nonsense'))
  }

  def "An empty blob store will remove the container"() {
    expect:
      azureClient.containerExists()
      !azureBlobStore.blobIdStream.findAny().isPresent()
    when:
      azureBlobStore.remove()
    then:
      !azureClient.containerExists()
  }
}
