package com.priyan.router.internal.aggregator;

import java.io.Serializable;
import java.util.Date;

public class AggregationAttributes implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String aggregationId;
  private final long firstItemArrivalTime;
  private final long lastItemArrivalTime;
  private final boolean aggregationComplete;
  private final int itemCount;

  public AggregationAttributes(String aggregationId, long firstItemArrivalTime,
                                long lastItemArrivalTime, boolean aggregationComplete, int itemCount) {
    this.aggregationId = aggregationId;
    this.firstItemArrivalTime = firstItemArrivalTime;
    this.lastItemArrivalTime = lastItemArrivalTime;
    this.aggregationComplete = aggregationComplete;
    this.itemCount = itemCount;
  }

  public String getAggregationId() {
    return aggregationId;
  }

  public Date getFirstItemArrivalTime() {
    return new Date(firstItemArrivalTime);
  }

  public Date getLastItemArrivalTime() {
    return new Date(lastItemArrivalTime);
  }

  public boolean isAggregationComplete() {
    return aggregationComplete;
  }

  public int getItemCount() {
    return itemCount;
  }
}
