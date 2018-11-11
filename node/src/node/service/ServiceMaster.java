package node.service;

import java.util.HashMap;
import java.util.Map;

public class ServiceMaster {

    private static final ServiceMaster INSTANCE = new ServiceMaster();

    private Map<String, Service> services = new HashMap<String, Service>();

    private ServiceMaster() {

    }

    public static ServiceMaster getInstance() {
        return INSTANCE;
    }

    public void addService(Service srv) {
        services.put(srv.getId(), srv);
    }

    public Service getService(String id) {
        return services.get(id);
    }
}
