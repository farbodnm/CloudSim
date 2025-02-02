package org.cloudbus.cloudsim.serverless.components.scheduling;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;

import java.util.List;

public class ServerlessContainerScheduler extends ContainerSchedulerTimeSharedOverSubscription {

    public ServerlessContainerScheduler(List<? extends ContainerPe> pelist) {
        super(pelist);
    }

    /**
     * Invoker functionalities
     */

    public boolean isSuitableForContainer(ServerlessContainer container, ServerlessInvoker invoker) {
        return true;
    }

    public boolean reAllocatePesForContainer(ServerlessContainer container, double newMips) {
        return true;
    }
}
