package org.sonatype.nexus.blobstore.azure.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Spliterator;

import org.sonatype.nexus.blobstore.azure.internal.InputStreamIterator.Tuple;

import io.reactivex.Flowable;

public class InputStreamIterator
    implements Iterable<Tuple<Flowable<ByteBuffer>, Long>>
{
  public static final int KB = 1024;

  public static final int MB = KB * 1024;

  private InputStream inputStream;

  public InputStreamIterator(final InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public Iterator<Tuple<Flowable<ByteBuffer>, Long>> iterator() {
    return new Iterator<Tuple<Flowable<ByteBuffer>, Long>>()
    {
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
      public Tuple<Flowable<ByteBuffer>, Long> next() {
        byte[] buffer = new byte[4 * MB];
        int read;
        try {
          read = inputStream.read(buffer);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        return new Tuple<>(Flowable.just(ByteBuffer.wrap(buffer, 0, read)), (long) read);
      }
    };
  }

  @Override
  public Spliterator spliterator() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public static class Tuple<A, B>
  {
    public A one;

    public B two;

    public Tuple(final A one, final B two) {
      this.one = one;
      this.two = two;
    }
  }
}
