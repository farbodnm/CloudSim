package org.cloudbus.cloudsim.serverless.components.scheduling;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;

import java.util.ArrayList;
import java.util.List;

/**
 * Container scheduler class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

public class ServerlessContainerScheduler extends ContainerSchedulerTimeSharedOverSubscription {

    public ServerlessContainerScheduler(List<? extends ContainerPe> pelist) {
        super(pelist);
    }

    /**
     * Invoker functionalities
     */

    public boolean isSuitableForContainer(ServerlessContainer container) {

        int assignedPes = 0;
        for (ContainerPe pe: getPeList()) {
            if (container.getMips() < pe.getContainerPeProvisioner().getAvailableMips()) {
                assignedPes++;
                if (assignedPes == container.getNumberOfPes()) {
                    break;
                }
            }
        }
        return assignedPes == container.getNumberOfPes();
    }

    public boolean reAllocatePesForContainer(ServerlessContainer container, double newMips) {

        boolean result = reAllocatePesForContainer(container.getUid(), newMips, container);
        updatePeProvisioning();
        return result;
    }

    /**
     * Local functionalities
     */

    private boolean reAllocatePesForContainer(String containerUid, double newMips, ServerlessContainer container) {

        double totalRequestedMips = 0;
        double oldMips = container.getMips() * container.getNumberOfPes();
        List<Double> mipsShareRequested = new ArrayList<>();
        for (int i = 0; i < container.getNumberOfPes(); i++) {
            mipsShareRequested.add(newMips);
            totalRequestedMips += newMips;
        }

        if (getContainersMigratingIn().contains(containerUid)) {
            totalRequestedMips = 0.0;
        } else {
            getMipsMapRequested().put(containerUid, mipsShareRequested);
        }

        if (getAvailableMips() >= totalRequestedMips - oldMips) {
            List<Double> mipsShareAllocated = new ArrayList<>();
            mipsShareRequested.forEach(mipsRequested -> {
                mipsShareAllocated.add(mipsRequested);
                container.setCurrentAllocatedMips(mipsShareRequested);
                container.setWorkloadMips(newMips);
                container.changeMips(newMips);
            });

            getMipsMap().put(containerUid, mipsShareAllocated);
            setAvailableMips(getAvailableMips() + oldMips - totalRequestedMips);
        } else {
            redistributeMipsDueToOverSubscription();
        }
        return true;
    }
}
