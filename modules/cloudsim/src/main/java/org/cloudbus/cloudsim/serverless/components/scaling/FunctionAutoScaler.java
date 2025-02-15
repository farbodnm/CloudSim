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
    protected final String CPU_ACTIONS = "cpuActions";
    protected final String MEMORY_ACTIONS = "memActions";

    private ServerlessDatacenter datacenter;
    private int userId;

    public FunctionAutoScaler(ServerlessDatacenter datacenter) {
        this.datacenter = datacenter;
    }

    /**
     * Abstractions
     */

    public abstract void scaleFunctions();
}
