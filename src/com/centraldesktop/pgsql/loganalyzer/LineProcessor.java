/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.centraldesktop.pgsql.loganalyzer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author trey
 */
public class LineProcessor implements Runnable {

    private static Pattern level = Pattern.compile("(\\w+):\\s*(.*)");
    private static Pattern clean_spaces = Pattern.compile("(\\^M|\\s+)");
    private static Pattern statement = Pattern.compile("duration: ([0-9\\.]+) ms (\\w+)\\s*([0-9a-zA-Z_\\<\\>\\/]*): (.+?) DETAIL: (.*)");
    private static Pattern no_detail_statement = Pattern.compile("duration: ([0-9\\.]+) ms (\\w+)\\s*([0-9a-zA-Z_\\<\\>\\/]*): (.+)");
    private static Pattern numeric_bare_value = Pattern.compile("(?:[\\-]?[0-9]+(?:\\.[0-9]+)?)");
    //private static Pattern text_bare_value = Pattern.compile("'(\\\\'|[^'])+'");
    private static Pattern text_bare_value = Pattern.compile("'(.*?(?<!\\\\))'");
    //private static Pattern text_bare_value = Pattern.compile("'([^']+?)'");
    private QueryRecorder recorder;
    private String line = null;
    private String query = null;
    private Double time = 0.0;
    private HashMap<String, Object> params = new HashMap<String, Object>();

    public LineProcessor(String line, QueryRecorder recorder) {
        this.line = line;
        this.recorder = recorder;
    }

    public void run() {
        Matcher lm = level.matcher(line);
        if (lm.find()) {
            String loglevel = lm.group(1);
            String rest = lm.group(2);

            if ("LOG".equals(loglevel)) {
                Matcher sm = clean_spaces.matcher(rest);
                rest = sm.replaceAll(" ");

                //System.out.println(rest);

                ArrayList<Pattern> patterns = new ArrayList<Pattern>();
                patterns.add(statement);
                patterns.add(no_detail_statement);

                for (Pattern p : patterns) {
                    Matcher s_m = p.matcher(rest);
                    if (s_m.find()) {
                        time = Double.parseDouble(s_m.group(1));
                        query = s_m.group(4);

                        normalize();

                        recorder.record(query, time, params);
                        break;
                    }
                }

//                if (query == null){
//                    System.out.println("Couldnt find query in: "+ rest);
//                }
            }
        } else {
            //System.err.println("No log level! " + line);
        }
    }

    private void normalize() {
        Matcher injected_text = text_bare_value.matcher(query);
        if (injected_text.find()) {
            query = injected_text.replaceAll("''");
        }
        
        Matcher nums = numeric_bare_value.matcher(query);
        if (nums.find()) {
            query = nums.replaceAll("0");
        }
    }
}
