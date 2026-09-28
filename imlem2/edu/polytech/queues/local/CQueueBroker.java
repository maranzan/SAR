package edu.polytech.queues.local;

import java.util.HashMap;
import java.util.Map;

import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.CTask;


public class CQueueBroker implements QueueBroker {

  private static final Map<String, CQueueBroker> brokers = new HashMap<String, CQueueBroker>();

  private static synchronized void register(CQueueBroker broker) {
    if (brokers.containsKey(broker.getName()))
      throw new IllegalStateException("broker deja existant : " + broker.getName());
    brokers.put(broker.getName(), broker);
  }

  static synchronized CQueueBroker lookup(String name) {
    return brokers.get(name);
  }

  public static synchronized void unregister(CQueueBroker broker) {
    brokers.remove(broker.getName());
  }

  private final String name;
  private final Task task;

  private final Map<Integer, BindListener> bindings = new HashMap<Integer, BindListener>();


  public CQueueBroker(String name) {
    if (name == null)
      throw new IllegalArgumentException("nom nul");
    Task current = CTask.task();
    if (current == null)
      throw new IllegalStateException("un broker doit être créé depuis une tâche");
    this.name = name;
    this.task = current;
    register(this);
  }
  
  
  public CQueueBroker(String name, Task task) {
    if (name == null || task == null)
      throw new IllegalArgumentException("nom ou tache nul");
    this.name = name;
    this.task = task;
    register(this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Task getTask() {
    return task;
  }

  @Override
  public boolean bind(int port, BindListener listener) {
    if (port < 0 || listener == null)
      return false;
    synchronized (bindings) {
      if (bindings.containsKey(port))
        return false;
      bindings.put(port, listener);
    }
    return true;
  }


  @Override
  public boolean unbind(int port) {
    final BindListener l;
    synchronized (bindings) {
      l = bindings.remove(port);
    }
    if (l == null)
      return false;
    task.post(new Runnable() {
      @Override
      public void run() {
        l.unbound();
      }
    });
    return true;
  }


  @Override
  public boolean connect(String name, final int port, final ConnectListener listener) {
    if (name == null || port < 0 || listener == null)
      return false;

    final CQueueBroker remote = lookup(name);
    if (remote == null)
      return false;

    final CQueueBroker local = this;


    remote.task.post(new Runnable() {
      @Override
      public void run() {
        BindListener bl;
        synchronized (remote.bindings) {
          bl = remote.bindings.get(port);
        }

        if (bl == null) {
          local.task.post(new Runnable() {
            @Override
            public void run() {
              listener.refused();
            }
          });
          return;
        }

        CMessageQueue acceptSide = new CMessageQueue(remote);
        final CMessageQueue connectSide = new CMessageQueue(local);
        acceptSide.setPeer(connectSide);
        connectSide.setPeer(acceptSide);

        bl.accepted(acceptSide);

        local.task.post(new Runnable() {
          @Override
          public void run() {
            listener.connected(connectSide);
          }
        });
      }
    });

    return true;
  }
}