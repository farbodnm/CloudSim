package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.enums.InvokerStatus;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.*;

/**
 * VM class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
@Slf4j
public class ServerlessInvoker extends PowerContainerVm {

    @Getter
    private final Map<String, ArrayList<Container>> functionContainerMap = new HashMap<>();

    @Getter
    private final Map<String, ArrayList<Container>> functionContainerMapPending = new HashMap<>();

    @Getter
    private final Map<String, Integer> vmTaskMap = new HashMap<>();

    @Getter
    private final ArrayList<ServerlessRequest> runningRequestList = new ArrayList<>();

    @Getter
    private final Map<String, ArrayList<ServerlessRequest>> vmTaskExecutionMap = new HashMap<>();

    @Getter
    private final Map<String, ArrayList<ServerlessRequest>> vmTaskExecutionMapFull = new HashMap<>();

    @Getter
    @Setter
    private InvokerStatus status = null;

    @Getter
    @Setter
    private double recordTime = 0;

    public double onTime = 0;

    public double offTime = 0;

    public boolean used = false;

    private final CongestionTracker congestionTracker;

    public ServerlessInvoker(int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
        congestionTracker = new CongestionTracker();
    }

    /**
     * Public functionalities
     */

    public void addToFunctionContainerMap(Container container, String functionId) {

        if (!functionContainerMap.computeIfAbsent(functionId, k -> new ArrayList<>()).contains(container)) {
            functionContainerMap.get(functionId).add(container);
        }
    }

    public void addToFunctionContainerMapPending(Container container, String functionId) {

        if (!functionContainerMapPending.computeIfAbsent(functionId, k -> new ArrayList<>()).contains(container)) {
            functionContainerMapPending.get(functionId).add(container);
        }
    }


    /**
     * Scheduler functionalities
     */

    public boolean isSuitableForContainer(Container container, ServerlessInvoker vm) {

        return (((ServerlessContainerScheduler) getContainerScheduler()).isSuitableForContainer(container, vm)
                && getContainerRamProvisioner().isSuitableForContainer(container, container.getCurrentRequestedRam())
                && getContainerBwProvisioner().isSuitableForContainer(container, container.getCurrentRequestedBw()));
    }

    public void addToVmTaskExecutionMap(ServerlessRequest task) {

        vmTaskExecutionMapFull.computeIfAbsent(task.getRequestFunctionId(), k -> new ArrayList<>()).add(task);

        int count = vmTaskExecutionMap.computeIfAbsent(task.getRequestFunctionId(), k -> new ArrayList<>()).size();
        vmTaskExecutionMap.get(task.getRequestFunctionId()).add(task);

        if (count == Constants.WINDOW_SIZE) {
            vmTaskExecutionMap.get(task.getRequestFunctionId()).remove(0);
        }
    }

    public void recordOffload() {
        congestionTracker.update(true, runningRequestList.isEmpty());
    }

    public double getCongestionFactor() {
        return congestionTracker.getCongestionFactor();
    }

    @Override
    public void containerDestroy(Container container) {
        if (container != null) {
            containerDeallocate(container);
            getContainerList().remove(container);

            List<String> functionsToRemove = new ArrayList<>();

            for (Map.Entry<String, ArrayList<Container>> entry : functionContainerMap.entrySet()) {
                for (int i = 0; i < entry.getValue().size(); i++) {
                    if (entry.getValue().get(i) == container) {
                        entry.getValue().remove(i);
                        if (entry.getValue().isEmpty()) {
                            functionsToRemove.add(entry.getKey());
                        }
                        break;
                    }
                }
            }
            for (String s : functionsToRemove) {
                functionContainerMap.remove(s);
            }
            functionContainerMap.values().removeIf(Objects::isNull);
            log.info("{}: {}: Container: {} is deleted from the list of VM: {}",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), getId());

            while (getContainerList().contains(container)) {
                log.warn("{}: {}: Stuck waiting for container: {} to get removed from VM: {}",
                        CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), getId());
            }
            log.info("{}: {}: Container: {} was successfully removed from the VM: {}",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), getId());
        }
    }


    @SuppressWarnings("DuplicatedCode")
    @Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {

        double longestRunTimeVm;

        double returnTime;
        longestRunTimeVm = 0;
        if (mipsShare != null && !getContainerList().isEmpty()) {

            double smallerTime = Double.MAX_VALUE;
            for (Container container : getContainerList()) {
                double time = ((ServerlessContainer) container).updateContainerProcessing(
                        currentTime, getContainerScheduler().getAllocatedMipsForContainer(container), this
                );

                if ((time == 0 || time == Double.MAX_VALUE) && !Constants.FUNCTION_AUTOSCALING) {
                    if (!((ServerlessContainer) container).newContainer && !((ServerlessContainer) container).isIdling()) {
                        ((ServerlessDatacenter) (this.getHost().getDatacenter())).getContainersToDestroy().add(container);
                        if (Constants.CONTAINER_IDLING_ENABLED) {
                            ((ServerlessContainer) container).setIdleStartTime(CloudSim.clock());
                            ((ServerlessContainer) container).setIdling(true);
                        }
                    }
                }

                if (time > 0.0 && time < smallerTime) {
                    smallerTime = time;
                }

                double longestTime = ((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).getLongestRunTime();
                if (longestTime > longestRunTimeVm) {
                    longestRunTimeVm = longestTime;
                }
            }
            ServerlessDatacenter.getRunTimeVm().put(this.getId(), longestRunTimeVm);

            returnTime = smallerTime;
        } else {
            returnTime = 0.0;
        }

        if (currentTime > getPreviousTime() && (currentTime - 0.2) % getSchedulingInterval() == 0) {
            double utilization = 0;

            for (Container container : getContainerList()) {
                // The containers which are going to migrate to the vm shouldn't be added to the utilization
                if (!getContainersMigratingIn().contains(container)) {
                    returnTime = container.getContainerCloudletScheduler().getPreviousTime();
                    utilization += container.getTotalUtilizationOfCpu(returnTime);
                }
            }

            if (CloudSim.clock() != 0 || utilization != 0) {
                addUtilizationHistoryValue(utilization);
            }
            setPreviousTime(currentTime);
        }

        if (CloudSim.clock() >= congestionTracker.getPreviousTime() + Constants.DECAY_HALF_LIFE) {
            congestionTracker.update(false, runningRequestList.isEmpty());
        }
        return returnTime;
    }

    public boolean reallocateResourcesForContainer(Container container, int cpuChange, int memChange) {

        log.info("{}: {}: Container: {} original values: [ MIPS: {}, RAM: {} ]",
                CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), container.getMips(), container.getRam());

        float newRam = container.getRam() + memChange;
        double newMIPS = container.getMips() + cpuChange;
        log.info("{}: {}: Container: {} requested additional RAM: {}; available VM RAM: {}",
                CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), memChange, getContainerRamProvisioner().getAvailableVmRam());
        log.info("{}: {}: Container: {} requested additional MIPS: {}; available VM MIPS: {}",
                CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), cpuChange * container.getNumberOfPes(), getAvailableMips());

        if (container.getMips() + cpuChange > this.getMips()) {
            return false;
        } else {
            if (!(getContainerRamProvisioner().getAvailableVmRam() >= memChange)) {
                log.info("{}: {}: VM RAM not enough to reallocate for container: {}",
                        CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
                return false;
            } else if (!(getAvailableMips() >= cpuChange * container.getNumberOfPes())) {
                log.info("{}: {}: VM MIPS not enough to reallocate for container: {}",
                        CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
                return false;
            } else if (!getContainerRamProvisioner().allocateRamForContainer(container, newRam)) {
                log.info("{}: {}: VM RAM reallocation failed for container: {}",
                        CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
                return false;
            } else if (!((ServerlessContainerScheduler) getContainerScheduler()).reAllocatePesForContainer(container, newMIPS)) {
                log.info("{}: {}: VM MIPS reallocation failed for container: {}",
                        CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
                return false;
            }

            container.setRam(Math.round(newRam));
            ((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).setTotalMips(newMIPS);
            log.info("{}: {}: Available VM: {} resources: [ MIPS: {}. RAM: {} ]",
                    CloudSim.clock(), this.getClass().getSimpleName(), getId(), getAvailableMips(), getContainerRamProvisioner().getAvailableVmRam());
            log.info("{}: {}: Container: {} reallocated values: [ MIPS: {}, RAM: {} ]",
                    CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), container.getMips(), container.getRam());
            return true;
        }
    }
}
