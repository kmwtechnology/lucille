package com.kmwllc.lucille.util;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RecordingLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {

  ArrayList history = new ArrayList<E>();

  public void put(E e) throws InterruptedException {
    history.add(e);
    super.put(e);
  }

  public boolean offer(E e, long timeout, TimeUnit unit)
    throws InterruptedException {
    history.add(e);
    return super.offer(e, timeout, unit);
  }

  public boolean offer(E e) {
    history.add(e);
    return super.offer(e);
  }

  public ArrayList<E> getHistory() {
    return history;
  }
}
