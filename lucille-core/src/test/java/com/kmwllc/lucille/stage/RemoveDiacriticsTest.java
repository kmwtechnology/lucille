package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class RemoveDiacriticsTest {

  private StageFactory factory = StageFactory.of(RemoveDiacritics.class);

  @Test
  public void testAccentsAndDiacratics() throws StageException {
    Stage stage = factory.get("CleanTextTest/config.conf");
    Document doc = Document.create("id");
    doc.setField("foo", "āăąēîïĩíĝġńñšŝśûůŷ");

    stage.processDocument(doc);

    assertEquals(doc.getString("foo"), "āăąēîïĩíĝġńñšŝśûůŷ");
    assertEquals(doc.getString("bar"), "aaaeiiiiggnnsssuuy");
  }
}
