package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterCharacteristics;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.PowerContainerDatacenterCM;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ServerlessDatacenter extends PowerContainerDatacenterCM {

    private List<Container> containersToDestroy = new ArrayList<>();

    public ServerlessDatacenter(String name, ContainerDatacenterCharacteristics characteristics, ContainerVmAllocationPolicy vmAllocationPolicy, ContainerAllocationPolicy containerAllocationPolicy, List<Storage> storageList, double schedulingInterval, String experimentName, String logAddress, double vmStartupDelay, double containerStartupDelay) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
    }

    @Override
    public void updateCloudletProcessing() {
        if (CloudSim.clock() < Constants.STARTUP_TIME || CloudSim.clock() > getLastProcessTime()) {
            List<? extends ContainerHost> containerHosts = getVmAllocationPolicy().getContainerHostList();
            double smallerTime = Double.MAX_VALUE;
            for (ContainerHost containerHost: containerHosts) {
                double time = containerHost.updateContainerVmsProcessing(CloudSim.clock());
                if (time < smallerTime) {
                    smallerTime = time;
                }
            }
            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(CloudSim.clock());
            destroyIdleContainers();
        }
    }

    protected void destroyIdleContainers() {
        for (Container container: containersToDestroy) {
            ServerlessContainer serverlessContainer = (ServerlessContainer) container;
            if ((serverlessContainer.isFirstProcess())) {
                serverlessContainer.startIdleTime();
            } else {
                send(getId(), Constants.CONTAINER_IDLING_TIME, CloudSimTags.CONTAINER_DESTROY_ACK, serverlessContainer);
            }
        }
        containersToDestroy.clear();
    }
}
