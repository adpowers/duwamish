package org.andrewhitchcock.duwamish.util;

import java.util.Map;

import org.andrewhitchcock.duwamish.model.HaltDecider;

public class DefaultHaltDecider implements HaltDecider {
  @Override
  public boolean shouldHalt(long superstepNumber, Map<String, Object> accumulations) {
    boolean votedToHalt = (Boolean)accumulations.get(Accumulators.VOTE_TO_HALT);
    long messagesSent = (Long)accumulations.get(Accumulators.MESSAGE_COUNT);
    return votedToHalt && messagesSent == 0;
  }
}
