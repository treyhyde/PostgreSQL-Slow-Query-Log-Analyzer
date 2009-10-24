/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.centraldesktop.pgsql.loganalyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.InputStreamReader;

import java.io.PrintStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author trey
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
        String filename = args[0];
        File file = new File(filename);

        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 6, 2, TimeUnit.SECONDS, queue);
        QueryRecorder recorder = new QueryRecorder();
        LineCombiner combiner = new LineCombiner(recorder, executor);


        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line = null;
        Integer foo = 0;
        while ((line = reader.readLine()) != null) {
            combiner.process(line);
        }

        combiner.flush();


        while (!queue.isEmpty()) {
            System.out.println("Waiting for threads to finish, " + queue.size() + " left to process.");
            Thread.sleep(500);
        }

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);
        File total = new File("total.txt");
        File agg = new File("aggregate_time.txt");
        File avg = new File("average_time.txt");
        recorder.printByTotal(new PrintStream(total));
        recorder.printByAggregateRunTime(new PrintStream(agg));
        recorder.printByAverageRunTime(new PrintStream(avg));

        reader.close();
    }
}
