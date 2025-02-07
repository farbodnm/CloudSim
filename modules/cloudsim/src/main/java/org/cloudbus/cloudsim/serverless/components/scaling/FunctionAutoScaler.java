package org.cloudbus.cloudsim.serverless.components.scaling;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverlessbac.Constants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public abstract class FunctionAutoScaler {

    private ServerlessDatacenter datacenter;
    private int userId;

    public FunctionAutoScaler(ServerlessDatacenter datacenter) {
        this.datacenter = datacenter;
    }

    /**
     * Abstractions
     */

    public abstract void scaleFunctions();

    protected abstract Map.Entry<Map<String, Map<String, Double>>, Map<String, List<ServerlessContainer>>> containerScalingTrigger();

    protected abstract void containerHorizontalScaler(Map<String, Map<String, Double>> fnNestedMap, Map<String, List<ServerlessContainer>> emptyContainers);

    protected abstract Map<String, Map<String, List<Integer>>> containerVerticalAutoScaler();
}
