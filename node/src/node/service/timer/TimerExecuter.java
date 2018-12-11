package node.service.timer;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import node.execute.ControlExecuter;
import node.execute.Executor;
import node.pipeline.PipelineMaster;

class TimerExecuter implements ControlExecuter {

    private static final TimerExecuter INSTANCE = new TimerExecuter();

    private Map<Long, Map<String, String>> ts2pipelineNo = new ConcurrentHashMap<Long, Map<String, String>>();

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

        if (PipelineMaster.getInstance().isDeleted(pipelineNo)) {
            System.out.println(new Date() + "/" + "TimerExecuter.addNextTs.PipelineMaster.isDeleted:" + pipelineNo + "/"
                    + new Date(nextTs));
            return;
        }

        Map<String, String> pipelineNos = ts2pipelineNo.get(nextTs);
        if (pipelineNos == null) {
            pipelineNos = new ConcurrentHashMap<String, String>();
            ts2pipelineNo.put(nextTs, pipelineNos);
        }
        pipelineNos.put(pipelineNo, pipelineNo);
    }

    private void execute() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long currentTs = System.currentTimeMillis();
                
                Iterator<Entry<Long, Map<String, String>>> it = ts2pipelineNo.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, Map<String, String>> entry = it.next();
                    long ts = entry.getKey();
                    Map<String, String> pipelineNos = entry.getValue();
                    Iterator<Entry<String, String>> pipelineNoIt = pipelineNos.entrySet().iterator();
                    while (pipelineNoIt.hasNext()) {
                        Entry<String, String> pipelineNo = pipelineNoIt.next();
                        if (PipelineMaster.getInstance().isDeleted(pipelineNo.getKey())) {
                            pipelineNoIt.remove();
                        }
                    }

                    if (ts < currentTs) {
                        for (Entry<String, String> pipelineNoEntry : pipelineNos.entrySet()) {
                            String pipelineNo = pipelineNoEntry.getKey();

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
                PipelineMaster.getInstance().cleanTimeOutDeletedIds();
            }
        };
        executor.scheduleAtFixedRate(runnable, 0, 30 * 1000, TimeUnit.MILLISECONDS);
    }
}
