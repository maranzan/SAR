package edu.polytech.queues.local;

import edu.polytech.queues.Bootstrap;
import edu.polytech.queues.Task;
import edu.polytech.utils.CTask;

public class Boot implements Bootstrap {

  public Boot() { }

  @Override
  public Task newTask(Runnable r, String name) {
    CTask t = new CTask(name);
    t.post(r);
    return t;
  }
}