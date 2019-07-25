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
package org.sonatype.nexus.blobstore.azure.internal.db

import org.sonatype.nexus.blobstore.api.BlobId
import org.sonatype.nexus.orient.testsupport.DatabaseInstanceRule

import org.junit.Rule
import spock.lang.Shared
import spock.lang.Specification

class OrientDeletedBlobEntityAdapterTest
    extends Specification
{
  @Rule
  public DatabaseInstanceRule database = DatabaseInstanceRule.inMemory("azureTest")

  @Shared
  def underTest = new OrientDeletedBlobEntityAdapter()

  def 'verify supported db operations'() {
    given:
      def connection = database.instance.connect()

    when: 'adapter is registered'
      underTest.register(connection)
    then: 'class is available with no entities'
      !connection.browseClass(OrientDeletedBlobEntityAdapter.DB_CLASE_TYPE).hasNext()

    when: 'adapter creates entity'
      def entity = new BlobIdEntity()
      entity.blobId = '12345'
      underTest.addEntity(connection, entity)
    then: 'entity is present in db'
      connection.browseClass(OrientDeletedBlobEntityAdapter.DB_CLASE_TYPE).next() != null

    when: 'adapter browses entities'
      def browse = underTest.browse(connection).collect()
      browse.find { it.blobId == '12345' }
      browse.size() == 1
    and: 'more entities are present'
      for (def i = 0; i < 5; i++) {
        entity = new BlobIdEntity()
        entity.blobId = "0000${i}"
        underTest.addEntity(connection, entity)
      }
    then: 'adapter returns all entities in browse'
      underTest.browse(connection).collect().size() == 6

    when: 'adapter deletes an entity'
      underTest.delete(connection, new BlobId('12345'))
      browse = underTest.browse(connection).collect { it.blobId }

    then: 'entity has been removed'
      browse.containsAll(['00000', '00001', '00002', '00003', '00004'])
      !browse.contains('12345')

    cleanup:
      connection.close()
  }
}
