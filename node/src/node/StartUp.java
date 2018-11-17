package node;

import node.pipeline.PipelineLoad;
import node.service.ServiceMaster;
import node.service.cmd.CmdServiceLoad;
import node.service.timer.GregorianTimer;

public class StartUp {

	public static void main(String[] args) {

        GregorianTimer gregorian = new GregorianTimer();
        ServiceMaster.getInstance().addService(gregorian);

        CmdServiceLoad cmdServiceLoad = new CmdServiceLoad();
        cmdServiceLoad.loadCmdService();
        // ServiceMaster.getInstance().getService("hello").execute();

        PipelineLoad pipelineLoad = new PipelineLoad();
        pipelineLoad.loadCmdService();

		new Thread(new Server()).start();
	}

}
