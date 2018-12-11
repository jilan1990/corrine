package node.pipeline;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import node.execute.Executor;
import node.pipeline.model.Pipeline;

public class PipelineMaster {

    private static final PipelineMaster INSTANCE = new PipelineMaster();

    private Map<String, Long> deletedIds = new ConcurrentHashMap<String, Long>();

    private BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

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
        String id = pipeline.getId();
        System.out.println(new Date() + "/" + "PipelineMaster.addPipeline/" + id);

        if (deletedIds.containsKey(id)) {
            System.out.println(new Date() + "/" + "PipelineMaster.addPipeline.failed.deletedIds.containsKey/" + id);
            return;
        }
        if (Executor.getInstance().contains(id)) {
            System.out.println(new Date() + "/" + "PipelineMaster.addPipeline.failed.Executor.contains/" + id);
            return;
        }

        Executor.getInstance().addWorker(pipeline);

        boolean bsuccess = queue.offer(pipeline.getId());
        if (!bsuccess) {
            //
        }
    }

    private void execute() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        String pipelineNo = queue.take();
                        long currentTs = System.currentTimeMillis();
                        Map<String, Object> msg = new HashMap<String, Object>();
                        msg.put("ts", currentTs);
                        msg.put("pipelineNo", pipelineNo);
                        msg.put("index", 0);
                        msg.put("data", pipelineNo);
                        System.out.println(new Date(currentTs) + "/" + "PipelineMaster.execute:" + msg);
                        Executor.getInstance().addMsg(msg);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        executor.submit(runnable);
        executor.shutdown();
    }

    public void deletePipeline(String pipelineNo) {
        deletedIds.put(pipelineNo, System.currentTimeMillis());

        Executor.getInstance().deletePipeline(pipelineNo);
    }

    public boolean isDeleted(String pipelineNo) {
        return deletedIds.containsKey(pipelineNo);
    }

    public void cleanTimeOutDeletedIds() {
        Iterator<Entry<String, Long>> pipelineNoIt = deletedIds.entrySet().iterator();
        while (pipelineNoIt.hasNext()) {
            Entry<String, Long> pipelineNo = pipelineNoIt.next();
            if ((System.currentTimeMillis() - pipelineNo.getValue()) > 10 * 60 * 1000) {
                pipelineNoIt.remove();
            }
        }
    }
}
