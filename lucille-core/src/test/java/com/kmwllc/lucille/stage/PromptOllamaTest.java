package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.chat.OllamaChatMessage;
import io.github.ollama4j.models.chat.OllamaChatRequest;
import io.github.ollama4j.models.chat.OllamaChatResponseModel;
import io.github.ollama4j.models.chat.OllamaChatResult;
import java.util.List;
import org.junit.Test;
import org.mockito.MockedConstruction;

public class PromptOllamaTest {

  private final StageFactory factory = StageFactory.of(PromptOllama.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private final String firstEnronResponse = """
{
  "fraud": false,
  "summary": "Person is trying to hide something from their boss."
}
""";

  private final String secondEnronResponse = """
{
  "fraud": true,
  "summary": "Employee coerces another into juicing earnings results to bolster market confidence, assuring them the company is on their side."
}
""";

  private final String noJsonResponse = "Person is trying to hide something from their boss.";

  @Test
  public void testOllama() throws Exception {
    OllamaChatResult firstResult = createMockChatResultWithMessage(firstEnronResponse);
    OllamaChatResult secondResult = createMockChatResultWithMessage(secondEnronResponse);

    try (MockedConstruction<OllamaAPI> mockAPIConstruction = mockConstruction(OllamaAPI.class, (mockAPI, context) -> {
      when(mockAPI.chat(any()))
          .thenReturn(firstResult)
          .thenReturn(secondResult);
    })) {
      Stage stage = factory.get("PromptOllamaTest/example.conf");

      Document doc1 = Document.create("doc1");
      doc1.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
      doc1.setField("sender", "j@abcdef.com");

      Document doc2 = Document.create("doc2");
      doc2.setField("message", "We want to make sure we are reporting higher earnings for the next quarter. You have as much leeway as you need. CEO is very focused on beating projections to bolster confidence in the markets.");
      doc2.setField("sender", "e1Jamie@company.com");

      stage.processDocument(doc1);
      stage.processDocument(doc2);

      assertFalse(doc1.getBoolean("fraud"));
      assertEquals("Person is trying to hide something from their boss.", doc1.getString("summary"));

      assertTrue(doc2.getBoolean("fraud"));
      assertEquals("Employee coerces another into juicing earnings results to bolster market confidence, assuring them the company is on their side.", doc2.getString("summary"));
    }
  }

  @Test
  public void testFields() throws Exception {
    OllamaChatResult firstResult = createMockChatResultWithMessage(firstEnronResponse);
    OllamaChatResult secondResult = createMockChatResultWithMessage(secondEnronResponse);

    try (MockedConstruction<OllamaAPI> mockAPIConstruction = mockConstruction(OllamaAPI.class, (mockAPI, context) -> {
      when(mockAPI.chat(any()))
          // Verifying that "daySent", which is NOT specified in fields, is not present in the request JSON
          .then(invocation -> {
            OllamaChatRequest request = invocation.getArgument(0, OllamaChatRequest.class);
            String recentMessageContent = request.getMessages().get(1).getContent();

            if (stringJsonHasFields(recentMessageContent, "daySent")) {
              fail("The message content had fields it should not have had.");
            }

            return firstResult;
          })
          .then(invocation -> {
            OllamaChatRequest request = invocation.getArgument(0, OllamaChatRequest.class);
            String recentMessageContent = request.getMessages().get(2).getContent();

            if (stringJsonHasFields(recentMessageContent, "daySent")) {
              fail("The message content had fields it should not have had.");
            }

            return secondResult;
          });
    })) {
      Stage stage = factory.get("PromptOllamaTest/fields.conf");

      Document doc1 = Document.create("doc1");
      doc1.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
      doc1.setField("sender", "j@abcdef.com");
      doc1.setField("daySent", "March 20, 2000");

      Document doc2 = Document.create("doc2");
      doc2.setField("message",
          "We want to make sure we are reporting higher earnings for the next quarter. You have as much leeway as you need. CEO is very focused on beating projections to bolster confidence in the markets.");
      doc2.setField("sender", "e1Jamie@company.com");
      doc2.setField("daySent", "September 21st, 2003");

      stage.processDocument(doc1);
      stage.processDocument(doc2);
    }
  }

  // The LLM isn't outputting a JSON response. requireJSON is set to false by default.
  @Test
  public void testNoJsonPrompt() throws Exception {
    OllamaChatResult result = createMockChatResultWithMessage(noJsonResponse);

    Document doc = Document.create("doc1");
    doc.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
    doc.setField("sender", "j@abcdef.com");

    try (MockedConstruction<OllamaAPI> mockAPIConstruction = mockConstruction(OllamaAPI.class, (mockAPI, context) -> {
      when(mockAPI.chat(any())).thenReturn(result);
    })) {
      Stage stage = factory.get("PromptOllamaTest/noJsonPrompt.conf");

      stage.processDocument(doc);

      assertFalse(doc.has("fraud"));
      assertFalse(doc.has("summary"));
      assertEquals("Person is trying to hide something from their boss.", doc.getString("ollamaResponse"));
    }
  }

  @Test
  public void testRequireJson() throws Exception {
    OllamaChatResult firstResult = createMockChatResultWithMessage(firstEnronResponse);
    OllamaChatResult noJsonResult = createMockChatResultWithMessage(noJsonResponse);

    Document doc = Document.create("doc1");
    doc.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
    doc.setField("sender", "j@abcdef.com");

    try (MockedConstruction<OllamaAPI> mockAPIConstruction = mockConstruction(OllamaAPI.class, (mockAPI, context) -> {
      when(mockAPI.chat(any())).thenReturn(firstResult);
    })) {
      Stage stage = factory.get("PromptOllamaTest/example.conf");

      stage.processDocument(doc);

      assertFalse(doc.getBoolean("fraud"));
      assertEquals("Person is trying to hide something from their boss.", doc.getString("summary"));
    }

    try (MockedConstruction<OllamaAPI> mockAPIConstruction = mockConstruction(OllamaAPI.class, (mockAPI, context) -> {
      when(mockAPI.chat(any())).thenReturn(noJsonResult);
    })) {
      Stage noJsonStage = factory.get("PromptOllamaTest/noJsonPromptRequireJson.conf");
      assertThrows(StageException.class, () -> noJsonStage.processDocument(doc));
    }
  }

  @Test
  public void testUpdateMode() throws Exception {
    OllamaChatResult firstResult = createMockChatResultWithMessage(firstEnronResponse);
    OllamaChatResult secondResult = createMockChatResultWithMessage(secondEnronResponse);

    try (MockedConstruction<OllamaAPI> mockAPIConstruction = mockConstruction(OllamaAPI.class, (mockAPI, context) -> {
      when(mockAPI.chat(any()))
          .thenReturn(firstResult)
          .thenReturn(secondResult);
    })) {
      String doc1FirstSummary = "An employee is doing something potentially sketchy, but perhaps not.";

      Stage stage = factory.get("PromptOllamaTest/updateAppend.conf");

      Document doc1 = Document.create("doc1");
      doc1.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
      doc1.setField("sender", "j@abcdef.com");
      doc1.setField("fraud", false);
      doc1.setField("summary", doc1FirstSummary);

      Document doc2 = Document.create("doc2");
      doc2.setField("message", "We want to make sure we are reporting higher earnings for the next quarter. You have as much leeway as you need. CEO is very focused on beating projections to bolster confidence in the markets.");
      doc2.setField("sender", "e1Jamie@company.com");
      doc2.setField("fraud", true);

      stage.processDocument(doc1);
      stage.processDocument(doc2);

      assertEquals(List.of(false, false), doc1.getBooleanList("fraud"));
      assertEquals(List.of(doc1FirstSummary, "Person is trying to hide something from their boss."), doc1.getStringList("summary"));

      assertEquals(List.of(true, true), doc2.getBooleanList("fraud"));
      assertEquals(List.of("Employee coerces another into juicing earnings results to bolster market confidence, assuring them the company is on their side."), doc2.getStringList("summary"));
    }
  }


  private OllamaChatResult createMockChatResultWithMessage(String messageStr) {
    // Supporting the call to: chatResult.getResponseModel().getMessage().getContent()
    OllamaChatResult chatResult = mock(OllamaChatResult.class);
    OllamaChatResponseModel responseModel = mock(OllamaChatResponseModel.class);
    OllamaChatMessage chatMessage = mock(OllamaChatMessage.class);

    when(chatResult.getResponseModel()).thenReturn(responseModel);
    when(responseModel.getMessage()).thenReturn(chatMessage);
    when(chatMessage.getContent()).thenReturn(messageStr);

    return chatResult;
  }

  // Returns whether the given string, which should be json, has any of the given fields. Returns false
  // if JSON cannot be parsed from the given String.
  private boolean stringJsonHasFields(String stringJson, String... fields) {
    try {
      JsonNode json = mapper.readTree(stringJson);

      for (String field : fields) {
        if (json.has(field)) {
          return true;
        }
      }

      return false;
    } catch (JsonProcessingException e) {
      return false;
    }
  }
}
