package com.kmwllc.lucille.pinecone.stage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.pinecone.util.PineconeManager;
import com.kmwllc.lucille.stage.StageFactory;
import io.pinecone.clients.Index;
import io.pinecone.proto.ListItem;
import io.pinecone.proto.ListResponse;
import io.pinecone.proto.Pagination;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class EmitDeletedByPrefixTest {

  StageFactory factory = StageFactory.of(EmitDeletedByPrefix.class);

  @Test
  public void testInvalidIndexerConfigs() throws StageException {
    assertThrows(StageException.class, () -> factory.get("EmitDeletedByPrefixTest/invalidIndexer.conf"));
  }

  @Test
  public void testInvalidPineconeConfigs() {
    assertThrows(StageException.class, () -> factory.get("EmitDeletedByPrefixTest/invalidPinecone.conf"));
  }

  @Test
  public void testInvalidIndexState() {
    PineconeManager mockManager = mock(PineconeManager.class);
    when(mockManager.isClientStable()).thenReturn(false);
    when(mockManager.getIndexName()).thenReturn("test");

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
          .thenReturn(mockManager);

      assertThrows(StageException.class, () -> factory.get("EmitDeletedByPrefixTest/goodConfig.conf"));
    }
  }

  @Test
  public void testValidIndexState() throws StageException {
    PineconeManager mockManager = mock(PineconeManager.class);
    when(mockManager.isClientStable()).thenReturn(true);
    when(mockManager.getIndexName()).thenReturn("test");

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
          .thenReturn(mockManager);
      Stage stage = factory.get("EmitDeletedByPrefixTest/goodConfig.conf");
    } catch (StageException e) {
      fail();
    }
  }


  @Test
  public void testIgnoreNonMarkedDocuments() throws StageException {
    PineconeManager mockManager = mock(PineconeManager.class);
    when(mockManager.isClientStable()).thenReturn(true);
    when(mockManager.getIndexName()).thenReturn("test");
    Index mockIndex = Mockito.mock(Index.class);
    when(mockManager.getIndex()).thenReturn(mockIndex);
    ListResponse mockResponse = Mockito.mock(ListResponse.class);
    when(mockResponse.hasPagination()).thenReturn(false);
    when(mockIndex.list(anyString(), anyString())).thenReturn(mockResponse);
    ListItem item = Mockito.mock(ListItem.class);
    ListItem item2 = Mockito.mock(ListItem.class);
    ListItem item3 = Mockito.mock(ListItem.class);
    when(item.getId()).thenReturn("id1");
    when(item2.getId()).thenReturn("id2");
    when(item3.getId()).thenReturn("id3");
    when(mockResponse.getVectorsList()).thenReturn(Arrays.asList(item, item2, item3));

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
          .thenReturn(mockManager);


      Stage stage = factory.get("EmitDeletedByPrefixTest/goodConfig.conf");
      Document doc1 = Document.create("id");

      // if doc1 has been marked for delete, it should return iterator of 3 documents, but since unmarked, will return null
      assertNull(stage.processDocument(doc1));
    }
  }

  @Test
  public void testEmptyList() throws StageException {
    PineconeManager mockManager = mock(PineconeManager.class);
    when(mockManager.isClientStable()).thenReturn(true);
    when(mockManager.getIndexName()).thenReturn("test");
    Index mockIndex = Mockito.mock(Index.class);
    when(mockManager.getIndex()).thenReturn(mockIndex);
    ListResponse mockResponse = Mockito.mock(ListResponse.class);
    when(mockResponse.hasPagination()).thenReturn(false);
    when(mockIndex.list(anyString(), anyString())).thenReturn(mockResponse);
    when(mockResponse.getVectorsList()).thenReturn(new ArrayList<>());

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
          .thenReturn(mockManager);


      Stage stage = factory.get("EmitDeletedByPrefixTest/goodConfig.conf");
      Document doc1 = Document.create("id");
      doc1.setField("to_delete", "true");

      // doc1 has been marked for delete, listing of prefix should return an empty list and will return null
      assertNull(stage.processDocument(doc1));
    }
  }

  @Test
  public void testEmitPrefixDocuments() throws StageException {
      PineconeManager mockManager = mock(PineconeManager.class);
      when(mockManager.isClientStable()).thenReturn(true);
      when(mockManager.getIndexName()).thenReturn("test");
      Index mockIndex = Mockito.mock(Index.class);
      when(mockManager.getIndex()).thenReturn(mockIndex);
      ListResponse mockResponse = Mockito.mock(ListResponse.class);
      when(mockResponse.hasPagination()).thenReturn(false);
      when(mockIndex.list(anyString(), anyString())).thenReturn(mockResponse);
      ListItem item = Mockito.mock(ListItem.class);
      ListItem item2 = Mockito.mock(ListItem.class);
      ListItem item3 = Mockito.mock(ListItem.class);
      when(item.getId()).thenReturn("id1-1");
      when(item2.getId()).thenReturn("id1-2");
      when(item3.getId()).thenReturn("id1-3");
      when(mockResponse.getVectorsList()).thenReturn(Arrays.asList(item, item2, item3));

      try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
        mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
            .thenReturn(mockManager);


        Stage stage = factory.get("EmitDeletedByPrefixTest/goodConfig.conf");
        Document doc1 = Document.create("id1");
        doc1.setField("to_delete", "true");

        // if doc1 has been marked for delete, it should return iterator of 3 documents
        Iterator<Document> iterator = stage.processDocument(doc1);
        assertTrue(iterator.hasNext());
        Document doc11 = iterator.next();
        assertTrue(iterator.hasNext());
        Document doc12 = iterator.next();
        assertTrue(iterator.hasNext());
        Document doc13 = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals("id1-1", doc11.getId());
        assertEquals("id1-2", doc12.getId());
        assertEquals("id1-3", doc13.getId());
      }
  }

  @Test
  public void testEmitPrefixDocumentsWithPagination() throws StageException {
    PineconeManager mockManager = mock(PineconeManager.class);
    when(mockManager.isClientStable()).thenReturn(true);
    when(mockManager.getIndexName()).thenReturn("test");
    Index mockIndex = Mockito.mock(Index.class);
    when(mockManager.getIndex()).thenReturn(mockIndex);
    ListResponse mockResponse = Mockito.mock(ListResponse.class);
    ListResponse mockResponse2 = Mockito.mock(ListResponse.class);
    Pagination mockPagination = Mockito.mock(Pagination.class);
    when(mockPagination.getNext()).thenReturn("nextPage");
    when(mockResponse.hasPagination()).thenReturn(true);
    when(mockResponse.getPagination()).thenReturn(mockPagination);
    when(mockIndex.list(anyString(), anyString())).thenReturn(mockResponse);
    when(mockIndex.list(anyString(), anyString(), eq("nextPage"))).thenReturn(mockResponse2);
    ListItem item = Mockito.mock(ListItem.class);
    ListItem item2 = Mockito.mock(ListItem.class);
    ListItem item3 = Mockito.mock(ListItem.class);
    when(item.getId()).thenReturn("id1-1");
    when(item2.getId()).thenReturn("id1-2");
    when(item3.getId()).thenReturn("id1-3");
    when(mockResponse.getVectorsList()).thenReturn(Arrays.asList(item, item2));
    when(mockResponse2.getVectorsList()).thenReturn(Collections.singletonList(item3));
    when(mockResponse2.hasPagination()).thenReturn(false);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
          .thenReturn(mockManager);


      Stage stage = factory.get("EmitDeletedByPrefixTest/goodConfig.conf");
      Document doc1 = Document.create("id1");
      doc1.setField("to_delete", "true");

      // if doc1 has been marked for delete, it should return iterator of 3 documents
      Iterator<Document> iterator = stage.processDocument(doc1);
      assertTrue(iterator.hasNext());
      Document doc11 = iterator.next();
      assertTrue(iterator.hasNext());
      Document doc12 = iterator.next();
      assertTrue(iterator.hasNext());
      Document doc13 = iterator.next();
      assertFalse(iterator.hasNext());
      assertEquals("id1-1", doc11.getId());
      assertEquals("id1-2", doc12.getId());
      assertEquals("id1-3", doc13.getId());
    }
  }


  @Test
  public void testDropOriginal() throws StageException {
    PineconeManager mockManager = mock(PineconeManager.class);
    when(mockManager.isClientStable()).thenReturn(true);
    when(mockManager.getIndexName()).thenReturn("test");
    Index mockIndex = Mockito.mock(Index.class);
    when(mockManager.getIndex()).thenReturn(mockIndex);
    ListResponse mockResponse = Mockito.mock(ListResponse.class);
    when(mockResponse.hasPagination()).thenReturn(false);
    when(mockIndex.list(anyString(), anyString())).thenReturn(mockResponse);
    when(mockResponse.getVectorsList()).thenReturn(new ArrayList<>());

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getInstance(anyString(), anyString()))
          .thenReturn(mockManager);


      Stage stage = factory.get("EmitDeletedByPrefixTest/dropOriginal.conf");
      Document doc1 = Document.create("id1");
      doc1.setField("to_delete", "true");

      stage.processDocument(doc1);
      assertTrue(doc1.isDropped());

      // if document does not have to_delete or value is not the same as valueField, will ignore and not drop even if dropOriginal is true
      Document doc2 = Document.create("id2");
      doc2.setField("to_delete", "false");
      assertFalse(doc2.isDropped());

      Document doc3 = Document.create("id3");
      doc3.setField("to_deleted", "true");
      assertFalse(doc3.isDropped());

      Document doc4 = Document.create("id2");
      doc4.setField("to_deleted", "false");
      assertFalse(doc4.isDropped());
    }
  }
}