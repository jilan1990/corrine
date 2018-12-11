package node.execute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import node.pipeline.model.Dataflow;
import node.pipeline.model.Pipeline;
import node.service.ControlService;
import node.service.Service;
import node.service.ServiceMaster;

public class Worker {
    public static Map<String, Object> execute(Pipeline pipeline, Map<String, Object> msg) {

        Map<String, Object> result = new HashMap<String, Object>();

        List<Dataflow> dataflows = pipeline.getDataflows();
        Integer index = (Integer) msg.get("index");

        if (index >= dataflows.size()) {
            return result;
        }

        Dataflow dataflow = dataflows.get(index++);

        long ts = (long) msg.get("ts");

        String dataflowId = dataflow.getDataflowId();
        String params = dataflow.getParams();

        Service service = ServiceMaster.getInstance().getService(dataflowId);
        String data = (String) msg.get("data");
        String resultData = service.execute(ts, data, params);
        result.put("data", resultData);

        if (service instanceof ControlService) {
            return result;
        }

        result.put("index", index);
        return result;
    }

}
