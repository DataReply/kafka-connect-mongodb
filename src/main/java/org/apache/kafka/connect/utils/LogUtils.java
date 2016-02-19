package org.apache.kafka.connect.utils;

import org.slf4j.Logger;

import java.util.Map;

/**
 * @author Andrea Patelli
 */
public class LogUtils {
    public static void dumpConfiguration(Map<String, String> map, Logger log) {
        log.trace("Starting connector with configuration:");
        for (Map.Entry entry : map.entrySet()) {
            log.trace("{}: {}", entry.getKey(), entry.getValue());
        }
    }
}
