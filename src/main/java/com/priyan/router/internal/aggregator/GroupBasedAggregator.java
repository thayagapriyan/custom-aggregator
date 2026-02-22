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
 * Provides the group-based aggregation operation.
 *
 * <p>Collects events into named groups via an external HTTP API (using the user's
 * global HTTP Requester configuration) and completes the aggregation when the
 * group reaches a configured size or when an eviction timeout expires.</p>
 */
public class GroupBasedAggregator {

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupBasedAggregator.class);

  @Inject
  private ExtensionsClient extensionsClient;

  @DisplayName("Aggregate By Group")
  public void aggregateByGroup(
      @Config CustomaggregatorConfiguration config,
      @DisplayName("Group ID") @Summary("Unique identifier for the aggregation group") String groupId,
      @DisplayName("Group Size") @Summary("Number of items that trigger aggregation completion") int groupSize,
      @DisplayName("Eviction Time") @Summary("Timeout to force-complete incomplete groups (0 = disabled)")
      @Optional(defaultValue = "0") int evictionTime,
      @DisplayName("Eviction Time Unit")
      @Optional(defaultValue = "SECONDS") TimeUnit evictionTimeUnit,
      @Content Object content,
      AggregationCompleteRoute onComplete,
      @Optional IncrementalAggregationRoute incremental,
      RouterCompletionCallback callback) {

    String aggregatorName = config.getConfigId();
    String httpConfigName = config.getHttpRequesterConfig();

    if (groupId == null || groupId.trim().isEmpty()) {
      callback.error(new IllegalArgumentException(
          "groupId must not be null or empty for aggregator [" + aggregatorName + "]"));
      return;
    }
    if (groupSize <= 0) {
      callback.error(new IllegalArgumentException(
          "groupSize must be positive, got [" + groupSize + "] for aggregator [" + aggregatorName + "]"));
      return;
    }
    if (content == null) {
      callback.error(new IllegalArgumentException(
          "content must not be null for aggregator [" + aggregatorName + "] group [" + groupId + "]"));
      return;
    }

    try {
      // Add event to the group via HTTP POST
      String addEventPayload = buildAddEventPayload(aggregatorName, groupId, groupSize,
          evictionTime, evictionTimeUnit.name(), content);

      String addEventPath = "/aggregator/groups/" + encodePathParam(groupId) + "/events";
      executeHttpPost(httpConfigName, addEventPath, addEventPayload);

      LOGGER.debug("Event added to aggregator [{}] group [{}]", aggregatorName, groupId);

      // Execute incremental route if provided
      executeIncrementalRoute(incremental, aggregatorName, groupId);

      // Check group status via HTTP GET
      String statusPath = "/aggregator/groups/" + encodePathParam(groupId)
          + "/status?aggregatorName=" + encodePathParam(aggregatorName);
      String statusResponse = executeHttpGet(httpConfigName, statusPath);

      boolean isComplete = parseIsComplete(statusResponse);

      if (isComplete) {
        LOGGER.info("Group complete for aggregator [{}] group [{}]", aggregatorName, groupId);
        completeAggregation(httpConfigName, aggregatorName, groupId, onComplete, callback);
        return;
      }

      // Not yet complete
      callback.success(Result.builder().build());

    } catch (Exception e) {
      LOGGER.error("Error in group aggregation for aggregator [{}] group [{}]: {}",
          aggregatorName, groupId, e.getMessage(), e);
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
      LOGGER.warn("Failed to mark group [{}] as complete: {}", groupId, e.getMessage());
    }

    LOGGER.info("Aggregation complete for group [{}]: {} items collected",
        groupId, aggregatedEvents.size());

    AggregationAttributes attributes = new AggregationAttributes(
        groupId,
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
          LOGGER.error("Error in onComplete route for group [{}]: {}", groupId, error.getMessage(), error);
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
                                               String aggregatorName, String groupId) {
    if (incremental == null) {
      return;
    }
    try {
      incremental.getChain().process(
          result -> {},
          (error, previous) -> LOGGER.warn("Incremental route error for aggregator [{}] group [{}]: {}",
              aggregatorName, groupId, error.getMessage())
      );
    } catch (Exception e) {
      LOGGER.warn("Failed to execute incremental route for aggregator [{}] group [{}]: {}",
          aggregatorName, groupId, e.getMessage(), e);
    }
  }

  // ──────────────────────────────────────────────────
  // JSON helpers
  // ──────────────────────────────────────────────────

  private String buildAddEventPayload(String aggregatorName, String groupId, int groupSize,
                                       int evictionTime, String evictionTimeUnit, Object content) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"aggregatorName\":\"").append(escapeJson(aggregatorName)).append("\",");
    sb.append("\"groupId\":\"").append(escapeJson(groupId)).append("\",");
    sb.append("\"groupSize\":").append(groupSize).append(",");
    sb.append("\"evictionTime\":").append(evictionTime).append(",");
    sb.append("\"evictionTimeUnit\":\"").append(escapeJson(evictionTimeUnit)).append("\",");
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
