package org.andrewhitchcock.duwamish;

import java.util.Map;

public interface HaltDecider {
  public boolean shouldHalt(long superstepNumber, Map<String, Object> accumulations);
}
