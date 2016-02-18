package org.apache.kafka.connect.utils;

/**
 * General string utilities that are missing from the standard library and may commonly be
 * required by Connector or Task implementations.
 */
public class StringUtils {
    /**
     * Generate a String by appending all the @{elements}, converted to Strings, delimited by
     *
     * @param elements list of elements to concatenate
     * @param delim    delimiter to place between each element
     * @return the concatenated string with delimiters
     * @{delim}.
     */
    public static <T> String join(Iterable<T> elements, String delim) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (T elem : elements) {
            if (first) {
                first = false;
            } else {
                result.append(delim);
            }
            result.append(elem);
        }
        return result.toString();
    }
}