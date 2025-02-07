package org.cloudbus.cloudsim.serverless.components.scaling;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;

import java.util.List;
import java.util.Map;

@Setter
@Getter
public abstract class FunctionAutoScaler {

    protected final String CONTAINER_COUNT = "containerCount";
    protected final String CONTAINER_PENDING_COUNT = "pendingContainerCount";
    protected final String CONTAINER_CPU_UTIL = "cpuUtilization";
    protected final String CONTAINER_MIPS = "containerMips";
    protected final String CONTAINER_RAM = "containerRam";
    protected final String CONTAINER_PES = "containerPes";

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
