package node;

import node.pipeline.PipelineLoad;
import node.service.cmd.CmdServiceLoad;

public class StartUp {

	public static void main(String[] args) {
        CmdServiceLoad cmdServiceLoad = new CmdServiceLoad();
        cmdServiceLoad.loadCmdService();
        // ServiceMaster.getInstance().getService("hello").execute();

        PipelineLoad pipelineLoad = new PipelineLoad();
        pipelineLoad.loadCmdService();

		new Thread(new Server()).start();
	}

}
