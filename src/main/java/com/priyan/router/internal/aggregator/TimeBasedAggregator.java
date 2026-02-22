package com.priyan.router.internal.aggregator;

import com.priyan.router.internal.CustomaggregatorConfiguration;

import org.mule.sdk.api.client.ExtensionsClient;
import org.mule.sdk.api.annotation.param.Config;
import org.mule.sdk.api.annotation.param.Content;
import org.mule.sdk.api.annotation.param.Optional;
import org.mule.sdk.api.annotation.param.display.DisplayName;
import org.mule.sdk.api.annotation.param.display.Summary;
import org.mule.sdk.api.runtime.operation.Result;
import org.mule.sdk.api.runtime.process.RouterCompletionCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Provides the time-based aggregation operation.
 *
 * <p>Collects events over a time window via an external HTTP API (using the user's
 * global HTTP Requester configuration) and completes the aggregation when the
 * period expires or an optional maximum size is reached (whichever comes first).</p>
 */
public class TimeBasedAggregator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeBasedAggregator.class);
  private static final String GROUP_ID_SUFFIX = "_current";

  @Inject
  private ExtensionsClient extensionsClient;

  @DisplayName("Aggregate By Time")
  public void aggregateByTime(
      @Config CustomaggregatorConfiguration config,
      @DisplayName("Aggregator Name") @Summary("Unique name for this time-based aggregator instance")
      String aggregatorName,
      @DisplayName("Period") @Summary("Length of the aggregation time window") int period,
      @DisplayName("Period Unit") TimeUnit periodUnit,
      @DisplayName("Max Size") @Summary("Maximum items for early completion (0 = time-only, no size limit)")
      @Optional(defaultValue = "0") int maxSize,
      @Content Object content,
      AggregationCompleteRoute onComplete,
      @Optional IncrementalAggregationRoute incremental,
      RouterCompletionCallback callback) {

    String configName = config.getConfigId();
    String httpConfigName = config.getHttpRequesterConfig();

    if (aggregatorName == null || aggregatorName.trim().isEmpty()) {
      callback.error(new IllegalArgumentException(
          "aggregatorName must not be null or empty for config [" + configName + "]"));
      return;
    }
    if (period <= 0) {
      callback.error(new IllegalArgumentException(
          "period must be positive, got [" + period + "] for aggregator [" + aggregatorName + "]"));
      return;
    }
    if (periodUnit == null) {
      callback.error(new IllegalArgumentException(
          "periodUnit must not be null for aggregator [" + aggregatorName + "]"));
      return;
    }
    if (content == null) {
      callback.error(new IllegalArgumentException(
          "content must not be null for aggregator [" + aggregatorName + "]"));
      return;
    }
    if (maxSize < 0) {
      callback.error(new IllegalArgumentException(
          "maxSize must be >= 0, got [" + maxSize + "] for aggregator [" + aggregatorName + "]"));
      return;
    }

    String groupId = aggregatorName + GROUP_ID_SUFFIX;

    try {
      // Add event to the time-based group via HTTP POST
      String addEventPayload = buildAddEventPayload(aggregatorName, groupId, period,
          periodUnit.name(), maxSize, content);

      String addEventPath = "/aggregator/groups/" + encodePathParam(groupId) + "/events";
      executeHttpPost(httpConfigName, addEventPath, addEventPayload);

      LOGGER.debug("Event added to time-based aggregator [{}]", aggregatorName);

      // Execute incremental route if provided
      executeIncrementalRoute(incremental, aggregatorName);

      // Check group status via HTTP GET
      String statusPath = "/aggregator/groups/" + encodePathParam(groupId)
          + "/status?aggregatorName=" + encodePathParam(aggregatorName);
      String statusResponse = executeHttpGet(httpConfigName, statusPath);

      boolean isComplete = parseIsComplete(statusResponse);

      if (isComplete) {
        LOGGER.info("Aggregation complete for time-based aggregator [{}]", aggregatorName);
        completeAggregation(httpConfigName, aggregatorName, groupId, onComplete, callback);
        return;
      }

      // Not yet complete
      callback.success(Result.builder().build());

    } catch (Exception e) {
      LOGGER.error("Error in time-based aggregation for aggregator [{}]: {}",
          aggregatorName, e.getMessage(), e);
      callback.error(e);
    }
  }

  private void completeAggregation(String httpConfigName, String aggregatorName, String groupId,
                                    AggregationCompleteRoute onComplete,
                                    RouterCompletionCallback callback) throws Exception {
    // Fetch all aggregated events via HTTP GET
    String eventsPath = "/aggregator/groups/" + encodePathParam(groupId)
        + "/events?aggregatorName=" + encodePathParam(aggregatorName);
    String eventsBody = executeHttpGet(httpConfigName, eventsPath);

    List<Object> aggregatedEvents = parseEvents(eventsBody);
    long firstItemArrivalTime = parseTimestamp(eventsBody, "firstItemArrivalTime");
    long lastItemArrivalTime = parseTimestamp(eventsBody, "lastItemArrivalTime");

    // Mark group as complete via HTTP POST
    String completePath = "/aggregator/groups/" + encodePathParam(groupId)
        + "/complete?aggregatorName=" + encodePathParam(aggregatorName);
    try {
      executeHttpPost(httpConfigName, completePath, "");
    } catch (Exception e) {
      LOGGER.warn("Failed to mark aggregator [{}] as complete: {}", aggregatorName, e.getMessage());
    }

    LOGGER.info("Time-based aggregation complete for [{}]: {} items collected",
        aggregatorName, aggregatedEvents.size());

    AggregationAttributes attributes = new AggregationAttributes(
        aggregatorName,
        firstItemArrivalTime,
        lastItemArrivalTime,
        true,
        aggregatedEvents.size()
    );

    Result<Object, AggregationAttributes> result = Result.<Object, AggregationAttributes>builder()
        .output(aggregatedEvents)
        .attributes(attributes)
        .build();

    onComplete.getChain().process(
        result,
        routeResult -> callback.success(routeResult),
        (error, previous) -> {
          LOGGER.error("Error in onComplete route for aggregator [{}]: {}",
              aggregatorName, error.getMessage(), error);
          callback.error(error);
        }
    );
  }

  // ──────────────────────────────────────────────────────────
  // HTTP helpers using ExtensionsClient + user's HTTP config
  // ──────────────────────────────────────────────────────────

  private String executeHttpPost(String httpConfigName, String path, String body) throws Exception {
    Result<InputStream, Object> result = extensionsClient.<InputStream, Object>execute(
        "HTTP", "request",
        params -> params
            .withConfigRef(httpConfigName)
            .withParameter("method", "POST")
            .withParameter("path", path)
            .withParameter("body", body)
    ).get();
    return readResponse(result);
  }

  private String executeHttpGet(String httpConfigName, String path) throws Exception {
    Result<InputStream, Object> result = extensionsClient.<InputStream, Object>execute(
        "HTTP", "request",
        params -> params
            .withConfigRef(httpConfigName)
            .withParameter("method", "GET")
            .withParameter("path", path)
    ).get();
    return readResponse(result);
  }

  private static String readResponse(Result<InputStream, Object> result) throws Exception {
    InputStream is = result.getOutput();
    if (is == null) {
      return "";
    }
    try {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      is.close();
    }
  }

  // ──────────────────────────────────────────────────
  // Route helpers
  // ──────────────────────────────────────────────────

  private static void executeIncrementalRoute(IncrementalAggregationRoute incremental,
                                               String aggregatorName) {
    if (incremental == null) {
      return;
    }
    try {
      incremental.getChain().process(
          result -> {},
          (error, previous) -> LOGGER.warn("Incremental route error for aggregator [{}]: {}",
              aggregatorName, error.getMessage())
      );
    } catch (Exception e) {
      LOGGER.warn("Failed to execute incremental route for aggregator [{}]: {}",
          aggregatorName, e.getMessage(), e);
    }
  }

  // ──────────────────────────────────────────────────
  // JSON helpers
  // ──────────────────────────────────────────────────

  private String buildAddEventPayload(String aggregatorName, String groupId, int period,
                                       String periodUnit, int maxSize, Object content) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"aggregatorName\":\"").append(escapeJson(aggregatorName)).append("\",");
    sb.append("\"groupId\":\"").append(escapeJson(groupId)).append("\",");
    sb.append("\"period\":").append(period).append(",");
    sb.append("\"periodUnit\":\"").append(escapeJson(periodUnit)).append("\",");
    sb.append("\"maxSize\":").append(maxSize).append(",");
    sb.append("\"content\":").append(serializeContent(content));
    sb.append("}");
    return sb.toString();
  }

  private boolean parseIsComplete(String responseBody) {
    return responseBody.contains("\"isComplete\":true")
        || responseBody.contains("\"isComplete\": true");
  }

  private List<Object> parseEvents(String responseBody) {
    List<Object> events = new ArrayList<>();
    events.add(responseBody);
    return events;
  }

  private long parseTimestamp(String responseBody, String fieldName) {
    try {
      int idx = responseBody.indexOf("\"" + fieldName + "\"");
      if (idx >= 0) {
        int colonIdx = responseBody.indexOf(":", idx);
        int endIdx = responseBody.indexOf(",", colonIdx);
        if (endIdx < 0) {
          endIdx = responseBody.indexOf("}", colonIdx);
        }
        if (colonIdx >= 0 && endIdx > colonIdx) {
          String value = responseBody.substring(colonIdx + 1, endIdx).trim();
          return Long.parseLong(value);
        }
      }
    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to parse timestamp field [{}] from response", fieldName, e);
    }
    return System.currentTimeMillis();
  }

  private String serializeContent(Object content) {
    if (content == null) {
      return "null";
    }
    if (content instanceof String) {
      return "\"" + escapeJson(content.toString()) + "\"";
    }
    if (content instanceof Number || content instanceof Boolean) {
      return content.toString();
    }
    return "\"" + escapeJson(content.toString()) + "\"";
  }

  private static String escapeJson(String value) {
    if (value == null) return "";
    return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
  }

  private static String encodePathParam(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
