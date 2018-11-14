package node.pipeline;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import node.execute.Executor;
import node.pipeline.model.Pipeline;

public class PipelineMaster {

    private static final PipelineMaster INSTANCE = new PipelineMaster();

    private final long MIN_PERIOD = 60 * 1000;

    private Map<String, Pipeline> pipelines = new ConcurrentHashMap<String, Pipeline>();

    private PipelineMaster() {

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            execute();
        });
        executor.shutdown();
    }

    public static PipelineMaster getInstance() {
        return INSTANCE;
    }

    public void addPipeline(Pipeline pipeline) {
        pipelines.put(pipeline.getId(), pipeline);
        Executor.getInstance().addWorker(pipeline);
    }

    public Pipeline getPipeline(String id) {
        return pipelines.get(id);
    }

    private void execute() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Map<String, Set<Long>> pipelineNo2Ts = new HashMap<String, Set<Long>>();
        Map<String, Long> cache = new HashMap<String, Long>();
        cache.put("lastTs", 0l);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long currentTs = System.currentTimeMillis();

                Set<String> delSet = new HashSet<String>();
                for (Map.Entry<String, Pipeline> entry : pipelines.entrySet()) {
                    String pipelineNo = entry.getKey();
                    Pipeline pipeline = entry.getValue();
                    String dateStr = pipeline.getDate();
                    long period = pipeline.getPeriod();

                    long nextTs = getTsByDate(dateStr);
                    if (period >= MIN_PERIOD) {
                        continue;
                    }
                    Set<Long> tss = pipelineNo2Ts.get(pipelineNo);
                    if (tss == null) {
                        tss = new HashSet<Long>();
                        pipelineNo2Ts.put(pipelineNo, tss);
                    }
                    tss.add(nextTs);
                    delSet.add(pipelineNo);
                }
                
                for (Map.Entry<String, Pipeline> entry : pipelines.entrySet()) {
                    String pipelineNo = entry.getKey();
                    Pipeline pipeline = entry.getValue();
                    String dateStr = pipeline.getDate();
                    long period  = pipeline.getPeriod();

                    long nextTs = getTsByDate(dateStr);
                    if (period < MIN_PERIOD) {
                        continue;
                    }

                    long sub = currentTs - nextTs + period;
                    long times = sub / period;
                    nextTs += times * period;

                    Set<Long> tss = pipelineNo2Ts.get(pipelineNo);
                    if (tss == null) {
                        tss = new HashSet<Long>();
                        pipelineNo2Ts.put(pipelineNo, tss);
                    }
                    tss.add(nextTs);
                }
                System.out.println(new Date(currentTs) + "/" + pipelineNo2Ts);
                long lastTs = cache.get("lastTs");
                for (Map.Entry<String, Set<Long>> entry : pipelineNo2Ts.entrySet()) {
                    String pipelineNo = entry.getKey();
                    Set<Long> tss = entry.getValue();
                    for (long ts : tss) {
                        if (lastTs < ts && ts <= currentTs) {
                            Map<String, Object> msg = new HashMap<String, Object>();
                            msg.put("ts", ts);
                            msg.put("pipelineNo", pipelineNo);
                            msg.put("index", 0);
                            System.out.println("PipelineMaster.execute:" + msg);
                            Executor.getInstance().addMsg(msg);
                        }
                    }
                    Iterator<Long> it = tss.iterator();
                    while (it.hasNext()) {
                        long ts = it.next();
                        if (ts < lastTs) {
                            it.remove();
                        }
                    }
                }
                for (String pipelineNo : delSet) {
                    pipelines.remove(pipelineNo);
                }
                cache.put("lastTs", currentTs);
            }
        };
        executor.scheduleAtFixedRate(runnable, 0, 30 * 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * 1986/07/03 00:00:00
     * 
     * @param dateStr
     * @return
     */
    private long getTsByDate(String dateStr) {
        String[] ss = dateStr.split(" ");
        String[] dates = ss[0].split("/");
        String[] times = ss[1].split(":");
        Integer year = Integer.parseInt(dates[0]);
        Integer month = Integer.parseInt(dates[1]) - 1;
        Integer day = Integer.parseInt(dates[2]);

        Integer hour = Integer.parseInt(times[0]);
        Integer minute = Integer.parseInt(times[1]);

        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, day, hour, minute);
        long ts = calendar.getTime().getTime() - 60 * 1000;
        ts = ts / MIN_PERIOD * MIN_PERIOD;
        return ts;
    }
}
