package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class RandomVectorTest {
  private StageFactory factory = StageFactory.of(RandomVector.class);

  @Test
  public void testMalformedConfig() {
    assertThrows(StageException.class, () -> factory.get("RandomVectorTest/noField.conf"));
    assertThrows(StageException.class, () -> factory.get("RandomVectorTest/noDimensions.conf"));
  }

  @Test
  public void testProcessDocument() throws StageException {
    try (MockedConstruction<Random> rand = Mockito.mockConstruction(Random.class, (mock, context) -> {
      Mockito.when(mock.nextFloat()).thenReturn(5f);
    })) {
      Stage append = factory.get("RandomVectorTest/append.conf");
      Stage overwrite = factory.get("RandomVectorTest/overwrite.conf");
      Stage skip = factory.get("RandomVectorTest/skip.conf");
      Stage moreDest = factory.get("RandomVectorTest/more_dest.conf");
      Stage four = factory.get("RandomVectorTest/four.conf");
      Stage def = factory.get("RandomVectorTest/default.conf");

      Document doc1 = Document.create("doc1");
      Document doc2 = Document.create("doc2");
      Document doc3 = Document.create("doc3");
      Document doc4 = Document.create("doc4");
      Document doc5 = Document.create("doc5");
      Document doc6 = Document.create("doc6");

      doc1.setOrAdd("dest", 1f);
      doc2.setOrAdd("dest", 1f);
      doc3.setOrAdd("dest", 1f);
      doc4.setOrAdd("dest", 1f);
      doc5.setOrAdd("dest", 1f);
      doc6.setOrAdd("dest", 1f);

      append.processDocument(doc1);
      overwrite.processDocument(doc2);
      skip.processDocument(doc3);
      moreDest.processDocument(doc4);
      four.processDocument(doc5);
      def.processDocument(doc6);

      assertEquals("doc1", doc1.getId());
      assertEquals("doc2", doc2.getId());
      assertEquals("doc3", doc3.getId());
      assertEquals("doc4", doc4.getId());
      assertEquals("doc5", doc5.getId());
      assertEquals("doc6", doc6.getId());

      assertEquals(2, doc1.getFieldNames().size());
      assertEquals(2, doc2.getFieldNames().size());
      assertEquals(2, doc3.getFieldNames().size());
      assertEquals(3, doc4.getFieldNames().size());
      assertEquals(2, doc5.getFieldNames().size());
      assertEquals(2, doc6.getFieldNames().size());

      assertEquals(List.of(1f, 9f, 9f, 9f), doc1.getFloatList("dest"));
      assertEquals(List.of(9f, 9f, 9f), doc2.getFloatList("dest"));
      assertEquals(List.of(1f), doc3.getFloatList("dest"));
      assertEquals(List.of(9f, 9f, 9f), doc4.getFloatList("dest"));
      assertEquals(List.of(9f, 9f, 9f), doc4.getFloatList("dest2"));
      assertEquals(List.of(9f, 9f, 9f, 9f), doc5.getFloatList("dest"));
      assertEquals(List.of(9f, 9f, 9f), doc6.getFloatList("dest"));
    }
  }
}
