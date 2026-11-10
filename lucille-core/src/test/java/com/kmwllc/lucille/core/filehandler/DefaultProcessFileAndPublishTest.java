package com.kmwllc.lucille.core.filehandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.junit.Test;

/**
 * Tests the default {@link FileHandler#processFileAndPublish} against the iterator contract on {@link FileHandler#processFile},
 * using handlers that implement only <code>processFile</code>.
 */
public class DefaultProcessFileAndPublishTest {

  /** A handler that implements only processFile, as the contract allows, and returns the iterator it is given. */
  private static FileHandler handlerReturning(Supplier<Iterator<Document>> iterator) {
    return (inputStream, pathStr) -> iterator.get();
  }

  /** An iterator over the given steps: a Document is returned, a RuntimeException is thrown by next(). */
  private static Iterator<Document> steps(Object... steps) {
    return new Iterator<>() {
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i < steps.length;
      }

      @Override
      public Document next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Object step = steps[i++];
        if (step instanceof RuntimeException e) {
          throw e;
        }
        return (Document) step;
      }
    };
  }

  private static InputStream emptyStream() {
    return new ByteArrayInputStream(new byte[0]);
  }

  private static List<String> publishedIds(FileHandler handler) throws Exception {
    List<String> ids = new ArrayList<>();
    Publisher publisher = mock(Publisher.class);
    doAnswer(inv -> ids.add(((Document) inv.getArgument(0)).getId())).when(publisher).publish(any());
    handler.processFileAndPublish(publisher, emptyStream(), "test.dat");
    return ids;
  }

  @Test
  public void testPublishesEveryDocumentFromProcessFile() throws Exception {
    FileHandler handler = handlerReturning(() -> steps(Document.create("a"), Document.create("b")));

    assertEquals(List.of("a", "b"), publishedIds(handler));
  }

  @Test
  public void testABadRecordIsSkippedAndReadingCarriesOn() throws Exception {
    FileHandler handler = handlerReturning(() -> steps(Document.create("a"), new IllegalArgumentException("bad record"),
        null, Document.create("c")));

    assertEquals(List.of("a", "c"), publishedIds(handler));
  }

  @Test
  public void testAHasNextThatThrowsStopsTheFile() {
    IllegalStateException cause = new IllegalStateException("can't read");
    FileHandler handler = handlerReturning(() -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        throw cause;
      }

      @Override
      public Document next() {
        throw new NoSuchElementException();
      }
    });

    FileHandlerException e = assertThrows(FileHandlerException.class, () -> publishedIds(handler));
    assertSame(cause, e.getCause());
  }

  @Test
  public void testTooManyBadRecordsFromNextStopsTheFile() {
    BadRecordTracker tracker = new BadRecordTracker("test.dat");
    FileHandler handler = handlerReturning(() -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Document next() {
        tracker.bad(1, "bad record");
        return null;
      }
    });

    FileHandlerException e = assertThrows(FileHandlerException.class, () -> publishedIds(handler));
    assertEquals(BadRecordTracker.TooManyBadRecordsException.class, e.getCause().getClass());
  }

  @Test
  public void testAPublisherFailureIsTheCause() throws Exception {
    RuntimeException cause = new RuntimeException("publish failed");
    Publisher publisher = mock(Publisher.class);
    doThrow(cause).when(publisher).publish(any());
    FileHandler handler = handlerReturning(() -> steps(Document.create("a")));

    FileHandlerException e = assertThrows(FileHandlerException.class,
        () -> handler.processFileAndPublish(publisher, emptyStream(), "test.dat"));
    assertSame(cause, e.getCause());
  }

  @Test
  public void testAProcessFileFailureIsTheCause() {
    FileHandlerException cause = new FileHandlerException("can't set up");
    FileHandler handler = (inputStream, pathStr) -> {
      throw cause;
    };

    FileHandlerException e = assertThrows(FileHandlerException.class, () -> publishedIds(handler));
    assertSame(cause, e.getCause());
  }
}
