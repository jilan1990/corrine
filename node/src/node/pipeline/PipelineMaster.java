package node.pipeline;

import java.util.HashMap;
import java.util.Map;

public class PipelineMaster {

    private static final PipelineMaster INSTANCE = new PipelineMaster();

    private Map<String, Pipeline> pipelines = new HashMap<String, Pipeline>();

    private PipelineMaster() {

    }

    public static PipelineMaster getInstance() {
        return INSTANCE;
    }

    public void addPipeline(Pipeline pipeline) {
        pipelines.put(pipeline.getId(), pipeline);
    }

    public Pipeline getPipeline(String id) {
        return pipelines.get(id);
    }
}
