package node.pipeline;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import node.pipeline.model.Pipeline;

public class PipelineLoad {

    private String configDir = "pipeline";

    public void loadCmdService() {
        File exeDir = new File(configDir);
        File[] exeFiles = exeDir.listFiles();
        for (File f : exeFiles) {
            String filename = f.getName();
            if (!filename.endsWith("_pipeline.yaml")) {
                continue;
            }
            Pipeline pipeline = getPipeline(f);
            if (pipeline != null) {
                PipelineMaster.getInstance().addPipeline(pipeline);
            }
        }
    }

    private Pipeline getPipeline(File file) {

        String encoding = "GBK";

        Yaml yaml = new Yaml(new Constructor(Pipeline.class));

        try {
            FileInputStream inputStream = new FileInputStream(file);

            Pipeline customer = (Pipeline) yaml.load(inputStream);

            inputStream.close();

            return customer;
        } catch (IOException e) {
            return null;
        }
    }
}
