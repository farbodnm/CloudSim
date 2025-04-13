package org.cloudbus.cloudsim.serverless.components;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Container scheduler class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
@Slf4j
public class ServerlessContainerScheduler extends ContainerSchedulerTimeSharedOverSubscription {

    /**
     * Instantiates a new container scheduler time-shared.
     *
     * @param peList the peList
     */
    public ServerlessContainerScheduler(List<? extends ContainerPe> peList) {
        super(peList);
    }

    public boolean reAllocatePesForContainer(Container container, double newMIPS) {

        boolean result = reAllocatePesForContainer(container.getUid(), newMIPS, container);
        updatePeProvisioning();
        return result;
    }

    public boolean isSuitableForContainer(Container container, ServerlessInvoker vm) {

        int assignedPes = 0;
        for (ContainerPe pe : getPeList()) {
            log.info("{}: {}: Invoker:{} has {} mips avalable while {} mips is needed for the container: {}",
                CloudSim.clock(), this.getClass().getSimpleName(), vm.getId(), pe.getContainerPeProvisioner().getAvailableMips(),
                container.getId(), container.getMips());
            double tmp = (pe.getContainerPeProvisioner().getAvailableMips());
            if (tmp > container.getMips()) {
                assignedPes++ ;
                if(assignedPes == container.getNumberOfPes()){
                    break;
                }
            }
        }
        return assignedPes == container.getNumberOfPes();
    }

    public boolean reAllocatePesForContainer(String containerUid, double newMips, Container container) {
        double totalRequestedMips = 0;
        double oldMips = container.getMips()* container.getNumberOfPes();

        // if the requested mips is bigger than the capacity of a single PE, we cap
        // the request to the PE's capacity
        List<Double> mipsShareRequested = new ArrayList<>();
        for (int x=0; x < container.getNumberOfPes(); x++){
            mipsShareRequested.add(newMips);
            totalRequestedMips += newMips;
        }

        if (getContainersMigratingIn().contains(containerUid)) {
            // the destination host only experience 10% of the migrating VM's MIPS
            totalRequestedMips = 0.0;
        } else {
            getMipsMapRequested().put(containerUid, mipsShareRequested);
        }

        log.debug("{}: {}: All avilable mips before reallocation: {}, extra mips request: {}",
            CloudSim.clock(), this.getClass().getSimpleName(), getAvailableMips(), totalRequestedMips - oldMips);

        if (getAvailableMips() >= (totalRequestedMips-oldMips)) {
            List<Double> mipsShareAllocated = new ArrayList<>();
            for (Double mipsRequested : mipsShareRequested) {
                if (!getContainersMigratingIn().contains(containerUid)) {
                    // the destination host only experience 10% of the migrating VM's MIPS
                    mipsShareAllocated.add(mipsRequested);

                    // Add the current MIPS to container
                    container.setCurrentAllocatedMips(mipsShareRequested);
                    container.setWorkloadMips(newMips);
                    container.changeMips(newMips);
                }
            }

            getMipsMap().put(containerUid, mipsShareAllocated);
            setAvailableMips(getAvailableMips()+oldMips - totalRequestedMips);
            log.debug("{}: {}: Total remaining mips of all vm pes: {}",
                CloudSim.clock(), this.getClass().getSimpleName(), getAvailableMips());
        } else {
            redistributeMipsDueToOverSubscription();
        }
        return true;
    }
}