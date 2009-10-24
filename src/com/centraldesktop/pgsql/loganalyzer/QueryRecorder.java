/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.centraldesktop.pgsql.loganalyzer;

import java.io.PrintStream;
import java.util.HashMap;

/**
 *
 * @author trey
 */
public class QueryRecorder {

    // probably replaces this with a database eventually
    private static HashMap<String, HashMap<String, Object>> all = new HashMap<String, HashMap<String, Object>>();
    private final static Integer max_record_size = 1024 * 8;

    public synchronized void record(String query, Double duration, HashMap<String, Object> parameters) {
        if (all.containsKey(query)) {
            all.get(query).put("count",
                    ((Integer) all.get(query).get("count")) + 1);
            all.get(query).put("duration",
                    ((Double) all.get(query).get("duration")) + duration);

        } else {
            parameters.put("count", 1);
            parameters.put("duration", duration);
            all.put(query, parameters);
        }
    }

    void printByTotal(PrintStream out) {
        for (String key : all.keySet()) {
            Integer max = max_record_size;
            if (key.length() < max) {
                max = key.length();
            }
            out.println(all.get(key).get("count").toString() + "\t" + key.substring(0, max));
            out.flush();
        }
    }

    void printByAggregateRunTime(PrintStream out) {
        for (String key : all.keySet()) {

            Integer max = max_record_size;
            if (key.length() < max) {
                max = key.length();
            }
            out.println(all.get(key).get("duration").toString() + "\t" + key.substring(0, max));
            out.flush();
        }
    }

    void printByAverageRunTime(PrintStream out) {
        for (String key : all.keySet()) {

            Integer max = max_record_size;
            if (key.length() < max) {
                max = key.length();
            }
            out.println(((Double) all.get(key).get("duration")) / ((Integer) all.get(key).get("count")) + "\t" + all.get(key).get("count") + "\t" + key.substring(0, max));
            out.flush();
        }
    }
}
