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

import spock.lang.Specification

class AzureBlobAttributesTest
    extends Specification
{
  def "azure blob properties read from a properties file"() {
    given: 'the blob attributes'
      def content = '''creationTime=10000000\nsize=100000000\nsha1=asdf'''

      def azureClient = Mock(AzureClient)
      def subject = new AzureBlobAttributes(azureClient, 'key')
    when: 'the object does not exist'
      1 * azureClient.exists('key') >> false
    then: 'the attributes are not loaded'
      !subject.load()
    when: 'the object does exist'
      1 * azureClient.exists('key') >> true
      1 * azureClient.get('key') >> new ByteArrayInputStream(content.bytes)
    then: 'the attributes are loaded'
      subject.load()
    when: 'the attributes are stored'
      subject.store()
    then: 'the attributes are written to the backing attributes file'
      1 * azureClient.create('key', _ as InputStream)
  }
}
