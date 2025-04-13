package org.cloudbus.cloudsim.serverless.components.schedule;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.ServerlessDatacenter;
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

    public abstract ContainerVm findVmForContainer(Container container);

    @Override
    public boolean allocateVmForContainer(Container container, ContainerVm containerVm, List<ContainerVm> containerVmList) {
        setContainerVmList(containerVmList);

        if (containerVm == null) {
            log.error("{}: {}: No suitable invoker found for container: {}",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
            return false;
        }
        if (containerVm.containerCreate(container)) { // if vm has been successfully created in the host
            getContainerTable().put(container.getUid(), containerVm);
            log.info("{}: {}: Container: {} has been allocated to invoker: :{}",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), containerVm.getId());
            return true;
        }
        log.error("{}: {}: Creation of container: {} on the invoker: {} failed unexpectedly",
                CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), containerVm.getId());
        return false;
    }

    public List<Map<String, Object>> optimizeAllocation(List<? extends Container> containerList) {
        return null;
    }

    public void reallocateVmResourcesForContainer(Container container, ServerlessInvoker vm, int cpuChange, int memChange) {
        vm.reallocateResourcesForContainer(container, cpuChange, memChange);
    }
}