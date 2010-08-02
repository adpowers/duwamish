package org.andrewhitchcock.duwamish.model;

import java.util.Map;

public interface HaltDecider {
  public boolean shouldHalt(long superstepNumber, Map<String, Object> accumulations);
}
