package org.cloudbus.cloudsim.serverless.components.scaling;

import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DefaultFunctionAutoScaler extends FunctionAutoScaler {

    public DefaultFunctionAutoScaler(ServerlessDatacenter datacenter) {
        super(datacenter);
    }

    @Override
    public void scaleFunctions() {


    }

    @Override
    protected Map.Entry<Map<String, Map<String, Double>>, Map<String, List<ServerlessContainer>>> containerScalingTrigger() {
        return null;
    }

    @Override
    protected void containerHorizontalScaler(Map<String, Map<String, Double>> fnNestedMap, Map<String, List<ServerlessContainer>> emptyContainers) {

    }

    @Override
    protected Map<String, Map<String, List<Integer>>> containerVerticalAutoScaler() {
        return Collections.emptyMap();
    }
}
