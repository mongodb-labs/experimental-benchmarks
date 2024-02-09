/**
 * Copyright (c) 2015-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.generator.acknowledge;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A CounterGenerator that reports generated integers via lastInt()
 * only after they have been acknowledged.
 */
public class DefaultAcknowledgedCounterGenerator extends AcknowledgedCounterGenerator {
  /** The size of the window of pending id ack's. 2^20 = {@value} */
  static final int DEFAUL_WINDOW_SIZE = Integer.rotateLeft(1, 17);

  private final ConcurrentHashMap<Long,String> mapping = new ConcurrentHashMap<>();
  private final ReentrantLock lock;
  private final boolean[] window;
  private volatile long limit;
  private final int windowSize;
  /** The mask to use to turn an id into a slot in {@link #window}. */
  private final int windowMask;

  /**
   * Create a counter that starts at countstart.
   */
  public DefaultAcknowledgedCounterGenerator(long countstart, int windowSize) {
    super(countstart);
    if(windowSize < 1) throw new IllegalArgumentException("windo must be positive");
    this.windowSize = windowSize;
    lock = new ReentrantLock();
    window = new boolean[this.windowSize];
    windowMask = this.windowSize - 1;
    limit = countstart - 1;
    System.err.println("starting AcknowledgedCounterGenerator with limit " + limit);
  }

  /**
   * Create a counter that starts at countstart.
   */
  public DefaultAcknowledgedCounterGenerator(long countstart) {
    this(countstart, DEFAUL_WINDOW_SIZE);
  }

  /**
   * In this generator, the highest acknowledged counter value
   * (as opposed to the highest generated counter value).
   */
  @Override
  public Long lastValue() {
    return limit;
  }
  @Override
  public Long nextValue() {
    Long l = super.nextValue();
    mapping.put(l, Thread.currentThread().getName());
    if(l - limit > this.windowSize) {
      System.err.println("given out more elements than fit into window. Where did they go? Limit is " + limit);
    }
    return l;
  }

  /**
   * Make a generated counter value available via lastInt().
   */
  public void acknowledge(long value) {
    final int currentSlot = (int)(value & this.windowMask);
    if (window[currentSlot]) {
      System.err.println(mapping);
      System.err.println(lock.isLocked() + " --- " + lock.getHoldCount() + " --- " + lock.getQueueLength());
      Map<Thread,StackTraceElement[]> map = Thread.getAllStackTraces();
      Exception e = new Exception();
      for(Thread t : map.keySet()) {
        System.err.println("thread: " + t.getName());
        e.setStackTrace(map.get(t));
        e.printStackTrace(System.err);
      }
      throw new RuntimeException("Too many unacknowledged insertion keys. Limit is " + limit);
    }
    mapping.remove(value);
    window[currentSlot] = true;

    if (lock.tryLock()) {
      // long start = System.currentTimeMillis();
      // move a contiguous sequence from the window
      // over to the "limit" variable
      try {
        // Only loop through the entire window at most once.
        long beforeFirstSlot = (limit & this.windowMask);
        long index;
        for (index = limit + 1; index != beforeFirstSlot; ++index) {
          int slot = (int)(index & this.windowMask);
          if (!window[slot]) {
            break;
          }
          window[slot] = false;
        }
        limit = index - 1;
      } finally {
        lock.unlock();
        /* long end = System.currentTimeMillis();
        if(end - start > 0) {
          System.out.println("cleanup took: " + (end -start) + " milliseconds ");
        }
        */
      }
    }
  }
}
