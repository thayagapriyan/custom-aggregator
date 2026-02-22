package com.priyan.router.internal;

import com.priyan.router.internal.aggregator.GroupBasedAggregator;
import com.priyan.router.internal.aggregator.TimeBasedAggregator;
import org.mule.sdk.api.annotation.Operations;
import org.mule.sdk.api.annotation.param.Parameter;
import org.mule.sdk.api.annotation.param.Optional;
import org.mule.sdk.api.annotation.param.display.DisplayName;
import org.mule.sdk.api.annotation.param.display.Summary;

/**
 * Configuration for the Custom Aggregator extension.
 *
 * <p>The user provides a reference to an existing global HTTP Requester configuration
 * which the aggregator uses for all API calls. No separate connection management is needed.</p>
 */
@Operations({GroupBasedAggregator.class, TimeBasedAggregator.class})
public class CustomaggregatorConfiguration {

  @DisplayName("Aggregator Name")
  @Summary("Unique identifier for this aggregator instance")
  @Parameter
  private String configId;

  @DisplayName("HTTP Requester Configuration")
  @Summary("Name of the global HTTP Request configuration (http:request-config) to use for aggregator API calls")
  @Parameter
  private String httpRequesterConfig;

  @DisplayName("Request Timeout (seconds)")
  @Summary("Maximum time to wait for an HTTP response from the aggregator API")
  @Parameter
  @Optional(defaultValue = "30")
  private int requestTimeoutSeconds;

  public String getConfigId() {
    return configId;
  }

  public String getHttpRequesterConfig() {
    return httpRequesterConfig;
  }

  public int getRequestTimeoutSeconds() {
    return requestTimeoutSeconds;
  }
}
