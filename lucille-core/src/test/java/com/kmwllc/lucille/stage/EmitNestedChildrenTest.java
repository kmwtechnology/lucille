package com.kmwllc.lucille.stage;

import static junit.framework.TestCase.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Pipeline;
import com.kmwllc.lucille.core.PipelineException;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.IteratorUtils;
import org.junit.Test;

public class EmitNestedChildrenTest {

  private StageFactory factory = StageFactory.of(EmitNestedChildren.class);

  @Test
  public void testChildrenEmit() throws StageException {
    Stage stage = factory.get("EmitNestedChildrenTest/emitchild.conf");

    Document parent = Document.create("parentId");
    Document child = Document.create("child1");
    Document child2 = Document.create("child2");
    child.setField("parent", parent.getId());
    child2.setField("parent", parent.getId());

    parent.addChild(child);
    parent.addChild(child2);

    Iterator<Document> documents = stage.processDocument(parent);

    int count = 0;
    // check children content
    while (documents.hasNext()) {
      count++;
      Document document = documents.next();

      assertEquals("child" + count, document.getId());
      assertEquals("parentId", document.getString("parent"));
    }

    // check parent is not marked for dropped and has no children
    assertFalse(parent.hasChildren());
    assertFalse(parent.isDropped());
  }

  @Test
  public void testNoChildrenEmit() throws StageException {
    Stage stage = factory.get("EmitNestedChildrenTest/drop_parent.conf");
    Document parent = Document.create("parentId");

    Iterator<Document> documents = stage.processDocument(parent);
    // no children emitted
    assertNull(documents);

    // even if dropParent is set to true, if no children, should not drop main parent document
    assertFalse(parent.hasChildren());
    assertFalse(parent.isDropped());
  }

  @Test
  public void testParentDrop() throws StageException {
    Stage stage = factory.get("EmitNestedChildrenTest/drop_parent.conf");

    Document parent = Document.create("parentId");
    Document child = Document.create("child1");
    Document child2 = Document.create("child2");
    child.setField("parent", parent.getId());
    child2.setField("parent", parent.getId());

    parent.addChild(child);
    parent.addChild(child2);

    Iterator<Document> documents = stage.processDocument(parent);

    assertFalse(parent.hasChildren());
    assertTrue(parent.isDropped());
  }

  @Test
  public void testPipeline() throws Exception {
    Pipeline pipeline = new Pipeline();
    Stage stage = factory.get("EmitNestedChildrenTest/emitchild.conf");
    pipeline.addStage(stage);

    Document parent = Document.create("parentId");
    Document child = Document.create("child1");
    Document child2 = Document.create("child2");
    child.setField("parent", parent.getId());
    child2.setField("parent", parent.getId());

    parent.addChild(child);
    parent.addChild(child2);

    pipeline.startStages();
    List<Document> results = IteratorUtils.toList(pipeline.processDocument(parent));
    pipeline.stopStages();

    // 3 documents expected out of processing the parent document with 2 attached children
    assertEquals(3, results.size());
  }
}