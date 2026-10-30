package com.kmwllc.lucille.util;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RecordingLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {

  List<E> history = new ArrayList<>();

  public void put(E e) throws InterruptedException {
    super.put(e);
    history.add(e);
  }

  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    boolean result = super.offer(e, timeout, unit);
    if (result) {
      history.add(e);
    }
    return result;
  }

  public boolean offer(E e) {
    boolean result = super.offer(e);
    if (result) {
      history.add(e);
    }
    return result;
  }

  public List<E> getHistory() {
    return history;
  }
}
