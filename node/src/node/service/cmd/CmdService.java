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
    public void execute() {
        try {
            Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getId() {
        return id;
    }

}
