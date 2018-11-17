package node.service.cmd;

import java.io.IOException;

import node.service.Service;

public class CmdService implements Service {

    private String id;
    private String cmd;
    
    public CmdService(String id, String cmd)
    {
        this.id = id;
        this.cmd = cmd;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String execute(long ts, String data, String params) {
        try {
            Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
