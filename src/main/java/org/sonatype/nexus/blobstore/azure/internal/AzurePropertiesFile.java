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
package org.sonatype.nexus.blobstore.azure.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.sonatype.nexus.common.property.ImplicitSourcePropertiesFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Properties} representation stored in Azure Cloud Storage.
 */
public class AzurePropertiesFile
    extends ImplicitSourcePropertiesFile
{
  private static final Logger log = LoggerFactory.getLogger(AzurePropertiesFile.class);

  private AzureClient azureClient;

  private final String key;

  public AzurePropertiesFile(final AzureClient azureClient, final String key) {
    this.azureClient = checkNotNull(azureClient);
    this.key = checkNotNull(key);
  }

  public void load() throws IOException {
    log.debug("Loading properties: {}", key);
    try (InputStream is = azureClient.get(key)) {
      load(is);
    }
  }

  public void store() throws IOException {
    log.debug("Storing properties: {}", key);
    ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
    store(bufferStream, null);
    azureClient.create(key, new ByteArrayInputStream(bufferStream.toByteArray()));
  }

  public boolean exists() {
    return azureClient.exists(key);
  }

  public void remove() {
    azureClient.delete(key);
  }

  public String toString() {
    return getClass().getSimpleName() + "{" +
        "key=" + key +
        '}';
  }
}
