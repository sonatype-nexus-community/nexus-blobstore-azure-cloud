/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2017-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype
 * .com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License
 * Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are
 * trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.blobstore.azure.internal


import org.sonatype.nexus.blobstore.BlobIdLocationResolver
import org.sonatype.nexus.blobstore.DefaultBlobIdLocationResolver
import org.sonatype.nexus.blobstore.api.BlobId
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration
import org.sonatype.nexus.common.log.DryRunPrefix

import spock.lang.Specification

class AzureBlobStoreTest
    extends Specification
{

  def "It will create, get and delete blobs"() {
    given: 'An azure blob store configuration'

      BlobStoreConfiguration configuration = new BlobStoreConfiguration(
          name: 'azure',
          type: AzureBlobStore.TYPE,
          attributes: [
              'azure cloud storage': [
                  (AzureBlobStore.ACCOUNT_NAME_KEY)  : System.getProperty('nxrm.azure.accountName'),
                  (AzureBlobStore.ACCOUNT_KEY_KEY)   : System.getProperty('nxrm.azure.accountKey'),
                  (AzureBlobStore.CONTAINER_NAME_KEY): System.getProperty('nxrm.azure.containerName')
              ]
          ]
      )
    and: 'A started azure blob store'
      BlobIdLocationResolver resolver = new DefaultBlobIdLocationResolver()
      AzureBlobStoreMetricsStore storeMetrics = Mock(AzureBlobStoreMetricsStore)
      DryRunPrefix dryRunPrefix = new DryRunPrefix("dr")

      def azureBlobStore = new AzureBlobStore(new AzureStorageClientFactory(), resolver, storeMetrics, dryRunPrefix)
      azureBlobStore.init(configuration)
      azureBlobStore.start()

    when: 'A new blob is created'
      byte[] bytes = [0, 0, 0, 1] as byte[]
      def headers = [
          'BlobStore.blob-name' : 'foo',
          'BlobStore.created-by': 'foo'
      ]
      def blob = azureBlobStore.create(new ByteArrayInputStream(bytes), headers)

    then: 'Its blobId will be returned in the stream'
      Optional<BlobId> blobId = azureBlobStore.getBlobIdStream()
          .filter({ blobId -> blobId == blob.id })
          .findFirst()

      blobId.get().asUniqueString() ==~ /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/
  }
}
