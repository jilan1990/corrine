package node.pipeline.model;

import java.util.List;

import node.pipeline.model.Dataflow;

public class Pipeline {
    private String id;
    private String date;
    private long period;

    private List<Dataflow> dataflows;
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public List<Dataflow> getDataflows() {
        return dataflows;
    }

    public void setDataflows(List<Dataflow> dataflows) {
        this.dataflows = dataflows;
    }
}
