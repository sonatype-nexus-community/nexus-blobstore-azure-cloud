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

/**
 * Unit tests for {@link AzurePropertiesFile}.
 */
class AzurePropertiesFileTest
    extends Specification
{

  def 'properties files can be loaded and stored'() {
    given: 'a properties file'
      def initialPropertyText = 'myProperty=foo'
      def key = 'some_path.properties'

      def azureClient = Mock(AzureClient)
      def subject = new AzurePropertiesFile(azureClient, key)
    when: 'it is loaded'
      subject.load()
    then: 'then the client retrieves the object by the key'
      1 * azureClient.get(key) >> new ByteArrayInputStream(initialPropertyText.bytes)
    and: 'the properties file will contain the property'
      subject.get('myProperty') == 'foo'
    when: 'a new property is added'
      subject.put('myOtherProperty', 'bar')
    and: 'the properties are stored'
      subject.store()
    then: 'it will store both properties'
      1 * azureClient.create(key, { InputStream data ->
        def lines = data.readLines()
        lines.contains(initialPropertyText) && lines.contains('myOtherProperty=bar')
      })
  }

  def "properties files exist based on client"() {
    given: 'a properties file'
      def azureClient = Mock(AzureClient)
      def subject = new AzurePropertiesFile(azureClient, 'key')
    when: 'the client reports the object exists'
      1 * azureClient.exists('key') >> true
    then: 'the properties file exists'
      subject.exists()
    when: 'the client reports the object does not exist'
      1 * azureClient.exists('key') >> false
    then: 'the properties file does not exist'
      !subject.exists()
  }
}
