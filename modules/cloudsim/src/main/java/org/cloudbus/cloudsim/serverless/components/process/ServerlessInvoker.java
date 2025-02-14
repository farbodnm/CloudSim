package org.cloudbus.cloudsim.serverless.components.process;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessContainerScheduler;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.enums.InvokerStatus;
import org.cloudbus.cloudsim.serverlessbac.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * VM class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

@Getter
@Setter
@Slf4j
public class ServerlessInvoker extends PowerContainerVm {

    public double onTime = 0;
    public double offTime = 0;

    private double recordTime = 0;
    private boolean isUsed;
    private InvokerStatus status = null;

    private List<ServerlessRequest> runningRequestsList = new ArrayList<>();
    private Map<String, Integer> functionsMap = new HashMap<>();
    private Map<String, List<ServerlessContainer>> pendingFunctionContainerMap = new HashMap<>();
    private Map<String, List<ServerlessContainer>> functionContainersMap = new HashMap<>();
    private Map<String, List<ServerlessRequest>> requestExecutionMap = new HashMap<>();
    private Map<String, List<ServerlessRequest>> requestExecutionMapFull = new HashMap<>();
    private Map<String, Integer> requestMap = new HashMap<>();

    public ServerlessInvoker(int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    }

    /**
     * Overrides
     */

    @Override
    public boolean containerCreate(Container container) {

        if (getSize() < container.getSize()) {
            log.warn("{}: {}: Allocation of container: {} failed because of storage.",
                    CloudSim.clock(), getName(), container.getId());
            return false;
        } else if (!getContainerRamProvisioner().allocateRamForContainer(container, container.getCurrentRequestedRam())) {
            log.warn("{}: {}: Allocation of container: {} failed because of ram.",
                    CloudSim.clock(), getName(), container.getId());
            return false;
        } else if (!getContainerBwProvisioner().allocateBwForContainer(container, container.getCurrentRequestedBw())) {
            log.warn("{}: {}: Allocation of container: {} failed because of BW.",
                    CloudSim.clock(), getName(), container.getId());
            getContainerRamProvisioner().deallocateRamForContainer(container);
            return false;
        } else if (!getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips())) {
            log.warn("{}: {}: Allocation of container: {} failed because of MIPS.",
                    CloudSim.clock(), getName(), container.getId());
            getContainerRamProvisioner().deallocateRamForContainer(container);
            getContainerBwProvisioner().deallocateBwForContainer(container);
            return false;
        } else {
            setSize(getSize() - container.getSize());
            getContainerList().add(container);
            container.setVm(this);
            return true;
        }
    }

    @Override
    public void containerDestroy(Container container) {

        if (container != null) {
            containerDeallocate(container);
            getContainerList().remove(container);

            List<String> functionsToRemove = new ArrayList<>();
            getFunctionContainersMap().forEach((key, value) -> {
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i) == container) {
                        value.remove(i);
                        if (value.isEmpty()) {
                            functionsToRemove.add(key);
                        }
                        break;
                    }
                }
            });

            functionsToRemove.forEach(function -> functionContainersMap.remove(function));
            functionContainersMap.forEach((key, value) -> {
                functionContainersMap.get(key).remove(null);
            });

            log.info("{}: {}: Container: {} destroyed.", CloudSim.clock(), getName(), container.getId());
        }
    }

    @Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {

        double longestRunTime = 0;
        double returnTime;

        if (mipsShare != null && !getContainerList().isEmpty()) {
            double smallerTime = Double.MAX_VALUE;

            for (Container c: getContainerList()) {
                ServerlessContainer container = (ServerlessContainer) c;
                double time = container.updateContainerProcessing(
                        currentTime,
                        getContainerScheduler().getAllocatedMipsForContainer(container),
                        this
                );
                if (time > 0D && time < smallerTime) {
                    smallerTime = time;
                }
                ServerlessRequestScheduler requestScheduler = ((ServerlessRequestScheduler) container.getContainerCloudletScheduler());
                if (requestScheduler.getLongestContainerRunTime() > longestRunTime) {
                    longestRunTime = requestScheduler.getLongestContainerRunTime();
                }
            }

            ServerlessDatacenter.invokerRunTimes.put(this.getId(), longestRunTime);
            returnTime = smallerTime;
        } else {
            returnTime = 0D;
        }

        if (currentTime > getPreviousTime() && (currentTime - 0.2) % getSchedulingInterval() == 0) {
            double utilization = 0;
            for (Container container : getContainerList()) {
                if(!getContainersMigratingIn().contains(container)) {
                    returnTime = container.getContainerCloudletScheduler().getPreviousTime();
                    utilization += container.getTotalUtilizationOfCpu(returnTime);
                }
            }
            if (CloudSim.clock() != 0 || utilization != 0) {
                addUtilizationHistoryValue(utilization);
            }
            setPreviousTime(currentTime);
        }

        return returnTime;
    }

    /**
     * Controller functionalities
     */

    public void addToPendingFunctionContainerMap(ServerlessContainer container, String functionId) {
        addToFunctionMap(container, functionId, pendingFunctionContainerMap);
    }

    public void addToFunctionContainerMap(ServerlessContainer container, String functionId) {
        addToFunctionMap(container, functionId, functionContainersMap);
    }

    /**
     * Scheduler functionalities
     */

    // TODO: To be changed.
    public boolean isSuitableForContainer(ServerlessContainer container, ServerlessInvoker invoker) {
        return ((ServerlessContainerScheduler) getContainerScheduler()).isSuitableForContainer(container, invoker)
                && getContainerRamProvisioner().isSuitableForContainer(container, container.getCurrentRequestedRam())
                && getContainerBwProvisioner().isSuitableForContainer(container, container.getCurrentRequestedBw());
    }

    public void addToRequestExecutionMap(ServerlessRequest request) {

        String functionId = request.getFunctionId();
        addToRequestExecutionMap(request, functionId, requestExecutionMapFull);
        if (addToRequestExecutionMap(request, functionId, requestExecutionMap) == Constants.WINDOW_SIZE) {
            requestExecutionMap.get(functionId).remove(0);
        }
    }

    public boolean reallocateResourcesForContainer(ServerlessContainer container, int cpuChange, int memChange) {

        float newRam = container.getRam() + memChange;
        double newMips = container.getMips() + cpuChange;

        if (this.getMips() < newMips) {
            return false;
        } else {
            if (getContainerRamProvisioner().getAvailableVmRam() < memChange) {
                return false;
            } else if (this.getAvailableMips() < cpuChange * container.getNumberOfPes()) {
                return false;
            } else if (!getContainerRamProvisioner().allocateRamForContainer(container, newRam)) {
                return false;
            } else if (!((ServerlessContainerScheduler) getContainerScheduler()).reAllocatePesForContainer(container, newMips)) {
                return false;
            }
        }

        container.setRam(Math.round(newRam));
        ((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).setTotalMips(newMips);
        return true;
    }

    /**
     * Local functionalities
     */

    private void addToFunctionMap(ServerlessContainer container, String functionId, Map<String, List<ServerlessContainer>> containersMap) {

        if (!containersMap.containsKey(functionId)) {
            List<ServerlessContainer> containerList = new ArrayList<>();
            containerList.add(container);
            containersMap.put(functionId, containerList);
        } else {
            if (!containersMap.get(functionId).contains(container)) {
                containersMap.get(functionId).add(container);
            }
        }
    }

    private int addToRequestExecutionMap(ServerlessRequest request, String functionId, Map<String, List<ServerlessRequest>> requestExecutionMap) {

        int count = requestExecutionMap.containsKey(functionId) ? requestExecutionMap.get(functionId).size() : 0;
        if (count == 0) {
            List<ServerlessRequest> requestList = new ArrayList<>();
            requestList.add(request);
            requestExecutionMap.put(functionId, requestList);
        } else {
            requestExecutionMap.get(functionId).add(request);
        }

        return count;
    }

    private String getName() {
        return "invoker#" + getId();
    }
}
