/**
 * Copyright (c) 2023-2024 benchANT GmbH. All rights reserved.
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A CounterGenerator that reports naively reports the
 * highest acknowledged id without considering holes in
 * the sequence. This is sufficient for bulk loads.
 */
public class StupidAcknowledgedCounterGenerator extends AcknowledgedCounterGenerator {

  private final AtomicLong limit;

  /**
   * Create a counter that starts at countstart.
   */
  public StupidAcknowledgedCounterGenerator(long countstart) {
    super(countstart);
    limit = new AtomicLong(countstart - 1);
  }

  /**
   * In this generator, the highest acknowledged counter value
   * (as opposed to the highest generated counter value).
   */
  @Override
  public Long lastValue() {
    return limit.get();
  }

  /**
   * Make a generated counter value available via lastInt().
   */
  public void acknowledge(long value) {
    while(true) {
      long l = limit.get();
      if(l < value) {
        boolean b = limit.compareAndSet(l, value);
        if(!b) continue;
      } 
      return;
    }
  }
}
