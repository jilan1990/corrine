package node.service;

public interface Service {
    String getId();

    String execute(long ts, String data);
}
