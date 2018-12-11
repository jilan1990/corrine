package node.execute;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import node.pipeline.model.Pipeline;

public class Executor {

    private static final Executor INSTANCE = new Executor();

    ExecutorService executor = Executors.newScheduledThreadPool(5);

    private BlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<Map<String, Object>>();
    private Map<String, Pipeline> pipelines = new ConcurrentHashMap<String, Pipeline>();

    private Executor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            execute();
        });
        executor.shutdown();
    }

    public static Executor getInstance() {
        return INSTANCE;
    }

    public void addWorker(Pipeline pipeline) {
        pipelines.put(pipeline.getId(), pipeline);
    }

    public void deletePipeline(String pipelineNo) {
        pipelines.remove(pipelineNo);
    }

    public boolean contains(String id) {
        return pipelines.containsKey(id);
    }

    public void addMsg(Map<String, Object> msg) {
        boolean bsuccess = queue.offer(msg);
        if (!bsuccess) {
            //
        }
    }

    private void execute() {
        try {
            while (true) {
                Map<String, Object> msg = queue.take();
                System.out.println(new Date() + "/" + "Executor.execute:" + msg);
                Object pipelineNo = msg.get("pipelineNo");
                if (pipelineNo == null) {
                    continue;
                }
                Pipeline pipeline = pipelines.get(pipelineNo);
                if (pipeline == null) {
                    continue;
                }
                executor.submit(() -> {
                    Map<String, Object> result = Worker.execute(pipeline, msg);
                    addMsg(result);
                });
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
