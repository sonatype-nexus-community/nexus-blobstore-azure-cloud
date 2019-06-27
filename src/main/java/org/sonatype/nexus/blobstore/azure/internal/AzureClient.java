package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface AzureClient
{
  void create(String path, InputStream data);

  InputStream get(String path) throws IOException;

  boolean exists(String path);

  void delete(String path);

  void copy(String sourcePath, String destination);

  Stream<String> listFiles(String contentPrefix);

  Stream<String> listFiles(String contentPrefix, Predicate<String> blobSuffixFilter);

  void createContainer();

  void deleteContainer();

  boolean containerExists();

  String getContainerName();
}
