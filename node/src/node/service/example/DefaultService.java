package node.service.example;

import java.util.Date;

import node.service.Service;

public class DefaultService implements Service {

    @Override
    public String getId() {
        return "default";
    }

    @Override
    public String execute(long ts, String data, String params) {
        System.out.println(new Date(ts) + "/DefaultService/" + data + "/" + params);
        return null;
    }

}
