package node.pipeline.model;

import java.util.List;

public class Pipeline {
    private String id;
    // private String date;
    // private long period;

    private List<Dataflow> dataflows;
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Dataflow> getDataflows() {
        return dataflows;
    }

    public void setDataflows(List<Dataflow> dataflows) {
        this.dataflows = dataflows;
    }
}
