/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.centraldesktop.pgsql.loganalyzer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author trey
 */
public class LineCombiner {

    private Executor executor;
    private QueryRecorder recorder;
    private static Pattern logline = Pattern.compile("\\[(\\d+)\\]: \\[(\\d+)\\-(\\d+)\\]\\s+(.*?)(--.*)?$");
    private static final Map<String, HashMap<Integer, String>> buffer = new ConcurrentSkipListMap<String, HashMap<Integer, String>>();

    public LineCombiner(QueryRecorder recorder, Executor executor) {
        this.executor = executor;
        this.recorder = recorder;
    }

    public void process(String line) {
        Matcher m = logline.matcher(line);

        if (m.find()) {
            String messagenum = m.group(1) + '-' + m.group(2);
            Integer sublinenum = Integer.parseInt(m.group(3));

            // start of new log entry
            if (sublinenum == 1) {
                if (buffer.containsKey(messagenum)) {

                    Runnable lineprocessor = new LineProcessor(serialize_buffer(buffer.get(messagenum)), recorder);
                    executor.execute(lineprocessor);
                }

                buffer.put(messagenum, new HashMap<Integer, String>());
            } else if (!buffer.containsKey(messagenum)) {
                buffer.put(messagenum, new HashMap<Integer, String>());
            }

            String partial = m.group(4);
            try {
                buffer.get(messagenum).put(sublinenum, partial);
            } catch (NullPointerException e) {
                System.err.println("Started with a non 1 message number " + messagenum + " " + sublinenum);
            }

        } else {
            System.out.println("Line doesn't match");
        }

    }

    public void flush() {
        synchronized (buffer) {
            for (String key : buffer.keySet()) {
                Runnable lineprocessor = new LineProcessor(serialize_buffer(buffer.get(key)), recorder);
                executor.execute(lineprocessor);
                buffer.remove(key);
            }
        }
    }

    public static String serialize_buffer(HashMap<Integer, String> buffer) {
        LinkedList<Integer> keylist = new LinkedList<Integer>();
        keylist.addAll(buffer.keySet());

        java.util.Collections.sort(keylist);

        StringBuffer tmp = new StringBuffer();
        Integer previousKey = null;
        Boolean incomplete = false;
        Integer missing_lines = 0;
        for (Integer key : keylist) {
            if (previousKey != null && key != (previousKey + 1)) {
                //System.err.println("Line number is not 1 more than the last.  Current is " + key + ", previous was: " + previousKey);
                incomplete = true;
                missing_lines += key - previousKey - 1;
            } else if (previousKey == null && key != 1) {
                incomplete = true;
                missing_lines += key - 2;
            }

            tmp.append(' ').append(buffer.get(key));
            previousKey = key;
        }

        String warn = "";
        if (incomplete) {
            warn = "INCOMPLETE QUERY, MISSING " + missing_lines + " LINES: ";
        }
        return warn + tmp.toString();
    }
}
