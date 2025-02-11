package org.cloudbus.cloudsim.serverless.components.scheduling;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicy;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FunctionScheduler extends PowerContainerAllocationPolicy {
    @Override
    public List<Map<String, Object>> optimizeAllocation(List<? extends Container> containerList) {
        return Collections.emptyList();
    }

    @Override
    public boolean allocateVmForContainer(Container container, ContainerVm vm, List<ContainerVm> containerVmList) {
        return false;
    }

    public boolean reallocateVmResourcesForContainer(ServerlessContainer container, ServerlessInvoker invoker, int cpuChange, int memChange) {
        return invoker.reallocateResourcesForContainer(container, cpuChange, memChange);
    }
}
