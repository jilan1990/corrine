package node.execute;

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
    private Map<String, Worker> workers = new ConcurrentHashMap<String, Worker>();

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
        workers.put(pipeline.getId(), new Worker(pipeline));
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
                System.out.println("Executor.execute:" + msg);
                Object pipelineNo = msg.get("pipelineNo");
                if (pipelineNo == null) {
                    continue;
                }
                Worker worker = workers.get(pipelineNo);
                executor.submit(() -> {
                    Map<String, Object> result = worker.execute(msg);
                    addMsg(result);
                });
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
