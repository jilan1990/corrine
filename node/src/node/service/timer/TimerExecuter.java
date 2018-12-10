package node.service.timer;

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

import node.execute.ControlExecuter;
import node.execute.Executor;
import node.pipeline.PipelineMaster;

public class TimerExecuter implements ControlExecuter {

    private static final TimerExecuter INSTANCE = new TimerExecuter();

    private Map<Long, Set<String>> ts2pipelineNo = new ConcurrentHashMap<Long, Set<String>>();

    private TimerExecuter() {
        PipelineMaster.getInstance().addControlExecuter("TimerExecuter", this);

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

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long currentTs = System.currentTimeMillis();
                
                Iterator<Entry<Long, Set<String>>> it = ts2pipelineNo.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, Set<String>> entry = it.next();
                    long ts = entry.getKey();
                    Set<String> pipelineNos = entry.getValue();
                    Set<String> set = new HashSet<String>();
                    set.addAll(pipelineNos);
                    if (ts < currentTs) {
                        for (String pipelineNo : set) {
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

    @Override
    public void deletePipeline(String pipelineNo) {
        for (Map.Entry<Long, Set<String>> entry : ts2pipelineNo.entrySet()) {
            entry.getValue().remove(pipelineNo);
        }
    }
}
