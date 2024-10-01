package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import java.util.concurrent.Future;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Event.Type;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.kmwllc.lucille.core.Document;

public class KafkaMessengerTest {

  @Test
  public void testSendFailed() throws Exception {
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");
    try (MockedStatic<KafkaUtils> kafkaUtils = Mockito.mockStatic(KafkaUtils.class);
        MockedStatic<RandomStringUtils> randomUtils = Mockito.mockStatic(RandomStringUtils.class)) {
      Future<RecordMetadata> mockResult = Mockito.mock(Future.class);
      KafkaProducer<String, Document> mockProducer = (KafkaProducer<String, Document>) Mockito.mock(KafkaProducer.class);
      Mockito.when(mockProducer.send(Mockito.any())).thenReturn(mockResult);
      KafkaConsumer<String, KafkaDocument> mockConsumer = (KafkaConsumer<String, KafkaDocument>) Mockito.mock(KafkaConsumer.class);
      randomUtils.when(() -> RandomStringUtils.randomAlphanumeric(8)).thenReturn("random");
      kafkaUtils.when(() -> {
        KafkaUtils.createDocumentProducer(config);
      }).thenReturn(mockProducer);
      kafkaUtils.when(() -> {
        KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-worker-foo-random");
      }).thenReturn(mockConsumer);
      kafkaUtils.when(() -> {
        KafkaUtils.getFailTopicName("foo");
      }).thenReturn("foo_fail");
      KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, "foo");
      Document doc = Document.create("doc2");
      doc.setOrAdd("key1", "val1");
      doc.setOrAdd("key2", 10);

      messenger.sendFailed(doc);
      ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
      Mockito.verify(mockProducer, Mockito.times(1)).send(captor.capture());
      Mockito.verify(mockProducer, Mockito.times(1)).flush();
      Mockito.verify(mockResult, Mockito.times(1)).get();

      assertEquals("foo_fail", captor.getValue().topic());
      assertEquals("doc2", captor.getValue().key());
      Document after = (Document) captor.getValue().value();
      assertEquals("doc2", after.getId());
      assertEquals("val1", after.getString("key1"));
      assertEquals((Integer) 10, after.getInt("key2"));
    }
  }

  @Test
  public void testSendEvent() throws Exception {
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");
    Config config2 = ConfigFactory.load("WorkerTest/config.conf");
    try (MockedStatic<KafkaUtils> kafkaUtils = Mockito.mockStatic(KafkaUtils.class);
        MockedStatic<RandomStringUtils> randomUtils = Mockito.mockStatic(RandomStringUtils.class)) {
      Future<RecordMetadata> mockResult = Mockito.mock(Future.class);
      KafkaProducer<String, String> mockProducer = (KafkaProducer<String, String>) Mockito.mock(KafkaProducer.class);
      Mockito.when(mockProducer.send(Mockito.any())).thenReturn(mockResult);
      KafkaConsumer<String, KafkaDocument> mockConsumer = (KafkaConsumer<String, KafkaDocument>) Mockito.mock(KafkaConsumer.class);
      randomUtils.when(() -> RandomStringUtils.randomAlphanumeric(8)).thenReturn("random");
      kafkaUtils.when(() -> {
        KafkaUtils.createEventProducer(config);
      }).thenReturn(null);
      kafkaUtils.when(() -> {
        KafkaUtils.createEventProducer(config2);
      }).thenReturn(mockProducer);
      kafkaUtils.when(() -> {
        KafkaUtils.createDocumentConsumer(config2, "com.kmwllc.lucille-worker-foo2-random");
      }).thenReturn(mockConsumer);
      kafkaUtils.when(() -> {
        KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-worker-foo-random");
      }).thenReturn(mockConsumer);
      kafkaUtils.when(() -> {
        KafkaUtils.getEventTopicName(config, "foo", "id");
      }).thenReturn("foo_event_id");
      kafkaUtils.when(() -> {
        KafkaUtils.getEventTopicName(config2, "foo2", "id");
      }).thenReturn("foo2_event_id");
      KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, "foo");
      KafkaWorkerMessenger messenger2 = new KafkaWorkerMessenger(config2, "foo2");

      Event event = new Event("doc1", "id", "bar", Type.CREATE);
        
      messenger.sendEvent(event);
      messenger2.sendEvent(event);
      ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
      Mockito.verify(mockProducer, Mockito.times(1)).send(captor.capture());
      Mockito.verify(mockProducer, Mockito.times(1)).flush();
      Mockito.verify(mockResult, Mockito.times(1)).get();

      assertEquals("foo2_event_id", captor.getValue().topic());
      assertEquals("doc1", captor.getValue().key());
      Event after = Event.fromJsonString((String) captor.getValue().value());
      assertEquals(Event.Type.CREATE, after.getType());
      assertEquals("doc1", after.getDocumentId());
      assertEquals("bar", after.getMessage());
      assertEquals("id", after.getRunId());
    }
  }

  @Test
  public void testSendEvent2() throws Exception {
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");
    Config config2 = ConfigFactory.load("WorkerTest/config.conf");
    Document doc = Document.create("doc1");
    try (MockedStatic<KafkaUtils> kafkaUtils = Mockito.mockStatic(KafkaUtils.class);
        MockedStatic<RandomStringUtils> randomUtils = Mockito.mockStatic(RandomStringUtils.class)) {
      Future<RecordMetadata> mockResult = Mockito.mock(Future.class);
      KafkaProducer<String, String> mockProducer = (KafkaProducer<String, String>) Mockito.mock(KafkaProducer.class);
      Mockito.when(mockProducer.send(Mockito.any())).thenReturn(mockResult);
      KafkaConsumer<String, KafkaDocument> mockConsumer = (KafkaConsumer<String, KafkaDocument>) Mockito.mock(KafkaConsumer.class);
      randomUtils.when(() -> RandomStringUtils.randomAlphanumeric(8)).thenReturn("random");
      kafkaUtils.when(() -> {
        KafkaUtils.createEventProducer(config);
      }).thenReturn(null);
      kafkaUtils.when(() -> {
        KafkaUtils.createEventProducer(config2);
      }).thenReturn(mockProducer);
      kafkaUtils.when(() -> {
        KafkaUtils.createDocumentConsumer(config2, "com.kmwllc.lucille-worker-foo2-random");
      }).thenReturn(mockConsumer);
      kafkaUtils.when(() -> {
        KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-worker-foo-random");
      }).thenReturn(mockConsumer);
      kafkaUtils.when(() -> {
        KafkaUtils.getEventTopicName(config, "foo", doc.getRunId());
      }).thenReturn("foo_event_id");
      kafkaUtils.when(() -> {
        KafkaUtils.getEventTopicName(config2, "foo2", doc.getRunId());
      }).thenReturn("foo2_event_id");
      KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, "foo");
      KafkaWorkerMessenger messenger2 = new KafkaWorkerMessenger(config2, "foo2");;

      messenger.sendEvent(doc, "message", Event.Type.FAIL);
      messenger2.sendEvent(doc, "message", Event.Type.FAIL);

      ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
      Mockito.verify(mockProducer, Mockito.times(1)).send(captor.capture());
      Mockito.verify(mockProducer, Mockito.times(1)).flush();
      Mockito.verify(mockResult, Mockito.times(1)).get();

      assertEquals("foo2_event_id", captor.getValue().topic());
      assertEquals("doc1", captor.getValue().key());
      Event after = Event.fromJsonString((String) captor.getValue().value());
      assertEquals(Event.Type.FAIL, after.getType());
      assertEquals("doc1", after.getDocumentId());
      assertEquals("message", after.getMessage());
      assertEquals(doc.getRunId(), after.getRunId());
    }
  }
}
