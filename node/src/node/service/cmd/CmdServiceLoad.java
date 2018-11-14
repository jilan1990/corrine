package node.service.cmd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import node.service.Service;
import node.service.ServiceMaster;

public class CmdServiceLoad {
    private String exeConfigDir = "service";

    public void loadCmdService() {
        File exeDir = new File(exeConfigDir);
        File[] exeFiles = exeDir.listFiles();
        for (File f : exeFiles) {
            String filename = f.getName();
            if (!filename.endsWith("_cmd.properties")) {
                continue;
            }
            Service srv = getService(f);
            if (srv != null) {
                ServiceMaster.getInstance().addService(srv);
            }
        }
    }

    private Service getService(File file) {

        String encoding = "GBK";

        try {
            InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// ¿¼ÂÇµ½±àÂë¸ñÊ½
            BufferedReader bufferedReader = new BufferedReader(read);
            Properties prop = new Properties();
            prop.load(bufferedReader);
            String cmd = prop.getProperty("cmd");
            if (cmd == null) {
                return null;
            }
            String id = prop.getProperty("id");
            if (id == null) {
                id = cmd;
            }

            bufferedReader.close();
            read.close();

            return new CmdService(id, cmd);
        } catch (IOException e) {
            return null;
        }
    }
}
