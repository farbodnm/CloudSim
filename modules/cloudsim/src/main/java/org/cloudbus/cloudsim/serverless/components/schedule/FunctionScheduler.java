package org.cloudbus.cloudsim.serverless.components.schedule;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.ServerlessInvoker;

import java.util.List;
import java.util.Map;

/**
 * FunctionScheduler class for CloudSimServerless extension. This class represents the scheduling policy of a function instance on a VM
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

@Slf4j
public abstract class FunctionScheduler extends PowerContainerAllocationPolicy {

    public FunctionScheduler() {
        super();
    }

    /**
     * Overrides
     */

    @Override
    public List<Map<String, Object>> optimizeAllocation(List<? extends Container> containerList) {
        return null;
    }

    @Override
    public boolean allocateVmForContainer(Container container, ContainerVm invoker, List<ContainerVm> invokerList) {

        setContainerVmList(invokerList);
        if (invoker == null) {
            log.warn("{}: {}: No suitable invoker found for container: {}",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
            return false;
        }

        if (invoker.containerCreate(container)) {
            getContainerTable().put(container.getUid(), invoker);
            log.info("{}: {}: Container {} has been allocated to the invoker {}",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), invoker.getId());
            return true;
        }

        log.error("{}: {}: Creation of container {} on the invoker {} failed unexpectedly.",
                CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), invoker.getId());
        return false;
    }

    public void deallocateVmForContainer(Container container) {
        ContainerVm invoker = getContainerTable().get(container.getUid());
        if (invoker != null) {
            invoker.containerDestroy(container);
        }
    }

    /**
     * Public functionalities
     */

    public boolean reallocateVmResourcesForContainer(ServerlessContainer container, ServerlessInvoker invoker, int cpuChange, int memChange) {
        return invoker.reallocateResourcesForContainer(container, cpuChange, memChange);
    }

    /**
     * Abstractions
     */

    @Override
    public abstract ContainerVm findVmForContainer(Container container);
}
