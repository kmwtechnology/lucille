package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ParseJsonTest {
  private StageFactory factory = StageFactory.of(ParseJson.class);

  @Test
  public void test() throws Exception {
    Stage stage = factory.get("ParseJson/config.conf");
    Document doc = new Document("doc");
    try (InputStream in = ParseJsonTest.class.getClassLoader().getResourceAsStream("ParseJson/test.json")) {
      doc.setField("json", IOUtils.toString(in, StandardCharsets.UTF_8));
      stage.processDocument(doc);
      //Get the map so we can assert on type
      Map<String, Object> docMap = doc.asMap();

      assertThat(doc.getString("aNumber"), equalTo("1"));
      assertThat(docMap.get("aNumber"), equalTo(1));

      assertThat(doc.getString("menuId"), equalTo("file"));
      assertThat(docMap.get("menuId"), equalTo("file"));

      assertThat(doc.getString("menuValue"), equalTo("File"));
      assertThat(docMap.get("menuValue"), equalTo("File"));

      //defer to ParseDate stage
      assertThat(doc.getString("aDate"), equalTo("2021-12-10"));
      assertThat(doc.getString("aTime"), equalTo("2021-12-10T14:05:26Z"));
      assertThat(docMap.get("aDate"), equalTo("2021-12-10"));
      assertThat(docMap.get("aTime"), equalTo("2021-12-10T14:05:26Z"));


      assertThat(doc.getString("isFalse"), equalTo("false"));
      assertThat(doc.getString("isTrue"), equalTo("true"));
      assertThat(docMap.get("isFalse"), CoreMatchers.is(false));
      assertThat(docMap.get("isTrue"), CoreMatchers.is(true));

      assertThat(doc.getString("item1"), equalTo("1"));
      assertThat(doc.getString("item2"), equalTo("2"));
      assertThat(doc.getString("item3"), equalTo("3"));
      assertThat(doc.getString("item4"), equalTo("false"));
      assertThat(doc.getString("item5"), equalTo("true"));
      assertThat(doc.getString("item6"), equalTo("3.74"));
      assertThat(docMap.get("item1"), equalTo(1));
      assertThat(docMap.get("item2"), equalTo(2));
      assertThat(docMap.get("item3"), equalTo(3));
      assertThat(docMap.get("item4"), equalTo(false));
      assertThat(docMap.get("item5"), equalTo(true));
      assertThat(docMap.get("item6"), equalTo(3.74));

      assertThat(doc.getString("item7Id"), equalTo("foo-bar-23"));
      assertThat(doc.getString("item7Label"), equalTo("Foo Bar 23"));
      assertThat(docMap.get("item7Id"), equalTo("foo-bar-23"));
      assertThat(docMap.get("item7Label"), equalTo("Foo Bar 23"));

      assertThat(doc.isMultiValued("menuItems"), CoreMatchers.is(true));
      List<String> menuItemStrings = doc.getStringList("menuItems");
      assertThat(menuItemStrings.size(), CoreMatchers.is(3));
      List<Map<String, Object>> menuItems = (List<Map<String, Object>>) docMap.get("menuItems");
      assertThat(menuItems.size(), CoreMatchers.is(3));
      assertThat(menuItems.get(0).get("value"), equalTo("New"));
      assertThat(menuItems.get(0).get("onclick"), equalTo("CreateNewDoc()"));
      assertThat(menuItems.get(1).get("value"), equalTo("Open"));
      assertThat(menuItems.get(1).get("onclick"), equalTo("OpenDoc()"));
      assertThat(menuItems.get(2).get("value"), equalTo("Close"));
      assertThat(menuItems.get(2).get("onclick"), equalTo("CloseDoc()"));

      //$.menu.popup.menuitem[*].value should consolidate the value field from menuitem objects into a single multivalued field.
      assertThat(doc.isMultiValued("menuItemValues"), CoreMatchers.is(true));
      List<String> menuItemValues = doc.getStringList("menuItemValues");
      assertThat(menuItemValues.size(), CoreMatchers.is(3));
      assertThat(menuItemValues.get(0), equalTo("New"));
      assertThat(menuItemValues.get(1), equalTo("Open"));
      assertThat(menuItemValues.get(2), equalTo("Close"));

      assertThat(doc.isMultiValued("items"), CoreMatchers.is(true));
      List<String> itemStrings = doc.getStringList("items");
      assertThat(itemStrings.size(), CoreMatchers.is(7));
      List<Object> items = (List<Object>) docMap.get("items");
      assertThat(items.size(), CoreMatchers.is(7));

      assertThat(items.get(0), equalTo(1));
      assertThat(items.get(1), equalTo(2));
      assertThat(items.get(2), equalTo(3));
      assertThat(items.get(3), equalTo(false));
      assertThat(items.get(4), equalTo(true));
      assertThat(items.get(5), equalTo(3.74));

      Map<String, Object> item7 = (Map<String, Object>) items.get(6);
      assertThat(item7.get("id"), equalTo("foo-bar-23"));
      assertThat(item7.get("label"), equalTo("Foo Bar 23"));

      // "/items/6" and "/items".get(6) should be equivalent
      assertThat(item7, equalTo(docMap.get("item7")));
    }

  }
}
