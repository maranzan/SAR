package edu.polytech.queues.local;

import java.util.LinkedList;

import edu.polytech.queues.MessageQueue;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;


public class CMessageQueue implements MessageQueue {

  private final CQueueBroker broker;

  private CMessageQueue peer;

  private Listener listener;
  private Task listenerTask;

  private final LinkedList<byte[]> incoming = new LinkedList<byte[]>();

  private boolean closed;
  private boolean peerClosed;
  private boolean closeNotified;

  CMessageQueue(CQueueBroker broker) {
    this.broker = broker;
  }

  void setPeer(CMessageQueue peer) {
    this.peer = peer;
  }

  @Override
  public QueueBroker broker() {
    return broker;
  }

  @Override
  public void setListener(Listener l) {
    setListener(l, currentTask());
  }

  public void setListener(Listener l, Task t) {
    if (t == null)
      throw new IllegalArgumentException("tache nulle");
    synchronized (this) {
      this.listener = l;
      this.listenerTask = t;
    }

    pump();
  }


  private Task currentTask() {
    return broker.getTask();
  }

  @Override
  public boolean send(final byte[] bytes, final int offset, final int length,
      final SendListener l) {

    if (bytes == null || offset < 0 || length < 0 || offset + length > bytes.length)
      throw new IllegalArgumentException("plage invalide : offset=" + offset
          + " length=" + length);

    byte[] msg = null;
    boolean dropped;

    synchronized (this) {

      dropped = closed || peerClosed;
      if (!dropped) {
        msg = new byte[length];
        System.arraycopy(bytes, offset, msg, 0, length);
      }
    }

    if (!dropped)
      peer.deliver(msg);


    if (l != null) {
      broker.getTask().post(new Runnable() {
        @Override
        public void run() {
          l.sent(bytes, offset, length);
        }
      });
    }

    return !dropped;
  }

  private void deliver(byte[] msg) {
    synchronized (this) {
      if (closed)
        return;
      incoming.add(msg);
    }
    pump();
  }


  @Override
  public void close() {
    final Listener l;
    final Task t;
    boolean notify = false;

    synchronized (this) {
      if (closed)
        return;
      closed = true;
      incoming.clear(); // on ne delivrera plus rien
      l = listener;
      t = listenerTask;
      if (!closeNotified && l != null && t != null) {
        closeNotified = true;
        notify = true;
      }
    }

    if (notify) {
      t.post(new Runnable() {
        @Override
        public void run() {
          l.closed();
        }
      });
    }

    peer.peerHasClosed();
  }

  private void peerHasClosed() {
    synchronized (this) {
      if (closed)
        return;
      peerClosed = true;
    }
    pump();
  }

  @Override
  public synchronized boolean closed() {
    return closed;
  }

  private void pump() {
    final Task t;
    synchronized (this) {
      if (listener == null || listenerTask == null)
        return;
      t = listenerTask;
    }
    t.post(new Runnable() {
      @Override
      public void run() {
        drain();
      }
    });
  }


  private void drain() {
    while (true) {
      byte[] msg = null;
      Listener l;
      boolean notifyClose = false;

      synchronized (this) {
        if (closed)
          return;
        l = listener;
        if (l == null)
          return;

        if (!incoming.isEmpty()) {
          msg = incoming.removeFirst();
        } else if (peerClosed && !closeNotified) {
          closed = true;
          closeNotified = true;
          notifyClose = true;
        } else {
          return;
        }
      }

      if (notifyClose) {
        l.closed();
        return;
      }
      l.received(msg);
    }
  }
}