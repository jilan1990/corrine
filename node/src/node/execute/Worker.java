package node.execute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import node.pipeline.model.Dataflow;
import node.pipeline.model.Pipeline;
import node.service.Service;
import node.service.ServiceMaster;

public class Worker {
    private Pipeline pipeline;
    public Worker(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public Map<String, Object> execute(Map<String, Object> msg) {

        Map<String, Object> result = new HashMap<String, Object>();

        List<Dataflow> dataflows = pipeline.getDataflows();
        Integer index = (Integer) msg.get("index");

        if (index >= dataflows.size()) {
            return result;
        }

        Dataflow dataflow = dataflows.get(index++);

        long ts = (long) msg.get("ts");

        String dataflowId = dataflow.getDataflowId();
        Service service = ServiceMaster.getInstance().getService(dataflowId);
        String data = service.execute(ts, (String) msg.get("data"));

        msg.put("index", index);
        msg.put("data", data);
        return msg;
    }

}
