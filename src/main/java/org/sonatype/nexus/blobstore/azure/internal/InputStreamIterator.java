package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class InputStreamIterator
  implements Iterable
{
  private InputStream inputStream;

  public InputStreamIterator(final InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public Iterator<byte[]> iterator() {
    return new Iterator<byte[]>() {
      @Override
      public boolean hasNext() {
        try {
          return inputStream.available() > 0;
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] next() {
        return null;
      }
    };
  }

  @Override
  public void forEach(final Consumer action) {

  }

  @Override
  public Spliterator spliterator() {
    return null;
  }
}
