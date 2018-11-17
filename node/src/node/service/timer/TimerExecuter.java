package node.service.timer;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import node.execute.Executor;

public class TimerExecuter {

    private static final TimerExecuter INSTANCE = new TimerExecuter();

    private final long MIN_PERIOD = 60 * 1000;

    private Map<Long, Set<String>> ts2pipelineNo = new ConcurrentHashMap<Long, Set<String>>();

    private TimerExecuter() {

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            execute();
        });
        executor.shutdown();
    }

    public static TimerExecuter getInstance() {
        return INSTANCE;
    }

    public void addNextTs(String pipelineNo, long nextTs) {
        System.out.println(new Date() + "/" + "TimerExecuter.addNextTs:" + pipelineNo + "/" + new Date(nextTs));
        Set<String> pipelineNos = ts2pipelineNo.get(nextTs);
        if (pipelineNos == null) {
            pipelineNos = new HashSet<String>();
            ts2pipelineNo.put(nextTs, pipelineNos);
        }
        pipelineNos.add(pipelineNo);
    }

    private void execute() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Map<String, Long> cache = new HashMap<String, Long>();
        cache.put("lastTs", 0l);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long currentTs = System.currentTimeMillis();

                long lastTs = cache.get("lastTs");
                cache.put("lastTs", currentTs);
                
                Iterator<Entry<Long, Set<String>>> it = ts2pipelineNo.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, Set<String>> entry = it.next();
                    long ts = entry.getKey();
                    Set<String> pipelineNos = entry.getValue();
                    if (ts < currentTs) {
                        for (String pipelineNo : pipelineNos) {
                            Map<String, Object> tsmsg = new HashMap<String, Object>();
                            tsmsg.put("ts", currentTs);
                            tsmsg.put("pipelineNo", pipelineNo);
                            tsmsg.put("index", 0);
                            tsmsg.put("data", pipelineNo);
                            System.out.println(new Date(currentTs) + "/" + "TimerExecuter.execute.tsmsg:" + tsmsg);
                            Executor.getInstance().addMsg(tsmsg);

                            Map<String, Object> msg = new HashMap<String, Object>();
                            msg.put("ts", currentTs);
                            msg.put("pipelineNo", pipelineNo);
                            msg.put("index", 1);
                            System.out.println(new Date(currentTs) + "/" + "TimerExecuter.execute.msg:" + msg);
                            Executor.getInstance().addMsg(msg);
                        }
                        it.remove();
                    }
                }
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
