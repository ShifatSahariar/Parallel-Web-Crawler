package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class ParallelCrawler extends RecursiveTask {
    private  Clock clock;
    private  PageParserFactory parserFactory;
    private  List<Pattern> ignoredUrls;
    private String url;
    private Instant deadline;
    private int maxDepth;
    private ConcurrentMap<String, Integer> counts;
    private ConcurrentSkipListSet<String> visitedUrls;
    private ReentrantLock reentrantLock = new ReentrantLock();

    public ParallelCrawler(String url, Instant deadline,
                           int maxDepth, ConcurrentMap<String,
                            Integer> counts,
                           ConcurrentSkipListSet<String> visitedUrls,
                           Clock clock,PageParserFactory parserFactory,
                           List<Pattern> ignoredUrls) {
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.clock = clock;
        this.parserFactory =parserFactory;
        this.ignoredUrls =ignoredUrls;

    }

    @Override
    protected Boolean compute() {
        ArrayList<ParallelCrawler> subtasks = new ArrayList<>();

        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return false;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return false;
            }
        }
        // check if the visited URL is visiting multiple time
        try{
            reentrantLock.lock();
            if (visitedUrls.contains(url)) {
                return false;
            }
            visitedUrls.add(url);
        }
        catch (Exception exception){
            exception.getMessage();
        }
        finally {
            reentrantLock.unlock();
        }


        PageParser.Result result = parserFactory.get(url).parse();
        for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()){
            if (counts.containsKey(e.getKey())) {
                counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
            } else {
                counts.put(e.getKey(), e.getValue());
            }

        }
        for (String link : result.getLinks()) {
            subtasks.add(new ParallelCrawler(link, deadline, maxDepth -1, counts, visitedUrls,clock,parserFactory,ignoredUrls));
        }
        invokeAll(subtasks);
        return true;
    }
}
