package org.cloudbus.cloudsim.serverless.components.provision;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;

public class ServerlessContainerRamProvisioner extends ContainerRamProvisionerSimple {

    public ServerlessContainerRamProvisioner(float availableRam) {
        super(availableRam);
    }

    @Override
    public boolean allocateRamForContainer(Container container, float ram) {

        float oldRam = container.getCurrentAllocatedRam();
        if (getAvailableVmRam() + oldRam >= ram) {
            deallocateRamForContainer(container);
            setAvailableVmRam(getAvailableVmRam() - ram);
            getContainerRamTable().put(container.getUid(), ram);
            container.setCurrentAllocatedRam(ram);
            return true;
        } else {
            return false;
        }
    }
}
