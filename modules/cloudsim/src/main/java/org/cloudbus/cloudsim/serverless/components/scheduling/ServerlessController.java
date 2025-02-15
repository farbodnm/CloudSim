package org.cloudbus.cloudsim.serverless.components.scheduling;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterBroker;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.serverless.components.loadbalancer.RequestLoadBalancer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.utils.CloudSimSCTags;
import org.cloudbus.cloudsim.serverlessbac.Constants;
import org.cloudbus.cloudsim.util.MathUtil;

import java.util.*;

/**
 * Broker class for CloudSimSC extension. This class represents a broker (Service Provider)
 * who uses the Cloud data center.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 *
 * TODO: Add congestion factor.
 */

@Slf4j
@Getter
@Setter
public class ServerlessController extends ContainerDatacenterBroker {

    protected ServerlessDatacenter datacenter;
    private RequestLoadBalancer loadBalancer;

    protected List<ServerlessContainer> containerDestroyedList;
    protected List<ServerlessInvoker> invokerIdleList = new ArrayList<>();
    protected List<Double> averageInvokersUsageRecords = new ArrayList<>();
    protected List<Integer> invokersCountList = new ArrayList<>();
    protected List<Double> meanAverageInvokersUsageRecords = new ArrayList<>();
    protected List<Double> meanSumOfInvokersCounts = new ArrayList<>();

    protected Map<String, List<ServerlessInvoker>> functionInvokersMap = new HashMap<>();
    protected List<ServerlessRequest> requestsToSubmitOnContainerCreation = new ArrayList<>();
    private Map<ServerlessInvoker, List<ServerlessRequest>> invokerTempTimeMap = new HashMap<>();

    public Queue<Double> requestArrivalTimes = new LinkedList<>();
    public Queue<ServerlessRequest> requestQueue = new LinkedList<>();

    protected double requestSubmitClock = 0;
    private int containersMadeCounter = 1; // Used as id

    public int tasksReturnedCount = 0;

    public ServerlessController(String name, int overBookingfactor) throws Exception {
        super(name, overBookingfactor);
        containerDestroyedList = new ArrayList<>();
    }

    @Override
    public void startEntity() {
        super.startEntity();
        while(!requestArrivalTimes.isEmpty()) {
            send(
                    getId(),
                    requestArrivalTimes.remove(),
                    CloudSimSCTags.CLOUDLET_SUBMIT,
                    requestQueue.remove()
            );
        }
    }

    @Override
    protected void processOtherEvent(SimEvent ev){
        switch (ev.getTag()) {
            case CloudSimSCTags.CLOUDLET_SUBMIT:
                submitRequest(ev);
                break;
            case CloudSimSCTags.CLOUDLET_SUBMIT_ACK:
                processRequestSubmitAck(ev);
                break;
            case CloudSimSCTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev);
                break;
            case CloudSimSCTags.SCALED_CONTAINER:
                processScaledContainer(ev);
                break;
            case CloudSimSCTags.RECORD_CPU_USAGE:
                recordResourceUtilization(ev);
                break;
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    /**
     * Event processors
     */

    private void submitRequest(SimEvent ev) {

        ServerlessRequest request = (ServerlessRequest) ev.getData();
        log.info("{}: {}: Request: {} arrived.",
                CloudSim.clock(), getName(), request.getCloudletId());

        if (CloudSim.clock() == requestSubmitClock) {
            send(
                    getId(),
                    Constants.MINIMUM_INTERVAL_BETWEEN_TWO_CLOUDLET_SUBMISSIONS,
                    CloudSimSCTags.CLOUDLET_SUBMIT_ACK,
                    request
            );
        } else {
            datacenter.updateCloudletProcessing();
            // Submit to core cloudlet list
            getCloudletList().add(request);
            loadBalancer.routeRequest(request);
        }
    }

    @Override
    public void processContainerCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int invokerId = data[0];
        int containerId = data[1];
        int result = data[2];

        ServerlessContainer container = ContainerList.getById(getContainerList(), containerId);

        if (result == CloudSimSCTags.TRUE) {
            if (invokerId == -1) {
                log.error("{}: {}: A container was created on an invoker that doesn't exist.",
                        CloudSim.clock(), getName());
            } else {
                getContainersToVmsMap().put(containerId, invokerId); // core
                getContainersCreatedList().add(container); // core
                try {
                    if (container == null) {
                        throw new NullPointerException("the container was not found");
                    }
                    container.setStartTime(CloudSim.clock());
                    ServerlessInvoker invoker = ContainerVmList.getById(getVmsCreatedList(), invokerId);
                    if (invoker == null) {
                        throw new NullPointerException("no invoker was assigned to it");
                    }
                    invoker.getPendingFunctionContainerMap().get(container.getFunctionType()).remove(container);
                    invoker.addToFunctionContainerMap(container, container.getFunctionType());

                    int hostId = ContainerVmList.getById(getVmsCreatedList(), invokerId).getHost().getId();
                    log.info(
                            "{}: {}: Container with id: {} was created on invoker: {}, and host: {}.",
                            CloudSim.clock(), getName(), containerId, invokerId, hostId);

                    containersCreated++;
                } catch (NullPointerException ex) {
                    log.error("{}: {} A container sent a creation event but {}.",
                            CloudSim.clock(), getName(), ex.getMessage());
                }
            }
        } else {
            log.error("{}: {}: Failed to created container with id: {}.",
                    CloudSim.clock(), getName(), containerId);
        }

        incrementContainersAcks();
        List<ServerlessRequest> toRemove = new ArrayList<>();

        for (ServerlessRequest request: requestsToSubmitOnContainerCreation) {
            if (request.getContainerId() == containerId) {
                ServerlessInvoker invoker = ContainerVmList.getById(getVmsCreatedList(), invokerId); // Core
                if (invoker != null) {
                    addToInvokerRequestsMap(request, invoker);
                    invokerTempTimeMap.get(invoker).remove(request);
                    invoker.addToFunctionContainerMap(container, request.getFunctionId());
                    addToFunctionInvokersMap(ContainerVmList.getById(getVmsCreatedList(), invokerId), request.getFunctionId());
                    submitRequestToDC(request, invokerId, 0, containerId);
                    toRemove.add(request);
                }
            }
        }
        requestsToSubmitOnContainerCreation.removeAll(toRemove);
        toRemove.clear();
    }

    private void processRequestSubmitAck(SimEvent ev) {

        ServerlessRequest request = (ServerlessRequest) ev.getData();
        ServerlessContainer container = ContainerList.getById(getContainerList(), request.getContainerId());
        assert container != null;
        container.setNewContainer(false);
    }

    private void processContainerDestroy(SimEvent ev) {

        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int containerId = data[1];
        int result = data[2];
        int invokerId = data[3];

        if (result == CloudSimSCTags.TRUE) {
            if (ContainerVmList.getById(getVmsCreatedList(), invokerId).getContainerList().isEmpty()) {
                invokerIdleList.add(ContainerVmList.getById(getVmsCreatedList(), invokerId));
            }

            getContainersToVmsMap().remove(containerId);
            getContainersCreatedList().remove(ContainerList.getById(getContainersCreatedList(), containerId));
            getContainerDestroyedList().add(ContainerList.getById(getContainerList(), containerId));
            ((ServerlessContainer) ContainerList.getById(getContainerList(), containerId)).setFinishTime(CloudSim.clock());
            setContainersCreated(getContainersCreated() - 1);
        } else {
            log.error("{}: {}: Failed to destroy container with id: {}",
                    CloudSim.clock(), getName(), containerId);
        }
    }

    private void processScaledContainer(SimEvent ev) {

        String[] data = (String[]) ev.getData();
        int controllerId = Integer.parseInt(data[0]);
        String functionTypeId = data[1];
        double containerMips = Double.parseDouble(data[2]);
        int containerRam = (int) Double.parseDouble(data[3]);
        int containerPes = (int) Double.parseDouble(data[4]);

        ServerlessContainer container =
                new ServerlessContainer(
                        containersMadeCounter++,
                        controllerId,
                        functionTypeId,
                        containerMips,
                        containerPes,
                        containerRam,
                        Constants.CONTAINER_BW,
                        Constants.CONTAINER_SIZE,
                        "Xen",
                        new ServerlessRequestScheduler(
                                containerMips,
                                containerPes
                        ),
                        Constants.SCHEDULING_INTERVAL
                );
        getContainerList().add(container);
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);
        log.info("{}: {}: Event sent to create to scale container: {}.",
                CloudSim.clock(), getName(), container.getId());
    }

    private void recordResourceUtilization(SimEvent ignored) {

        double utilization;
        double utilizationSum = 0;
        int invokerCount = 0;

        for (ContainerVm vm: getVmsCreatedList()) {
            ServerlessInvoker invoker = (ServerlessInvoker) vm;
            utilization = 1 - invoker.getAvailableMips() / invoker.getTotalMips();
            if (utilization > 0) {
                invoker.setUsed(true);
                invokerCount++;
                utilizationSum += utilization;
            }
        }
        if (utilizationSum > 0) {
            averageInvokersUsageRecords.add(utilizationSum / invokerCount);
            invokersCountList.add(invokerCount);
        }

        double sumOfAverage = 0;
        double sumOfVmCount = 0;
        if (averageInvokersUsageRecords.size() >= Constants.CPU_HISTORY_LENGTH) {
            for (int i = 0; i < averageInvokersUsageRecords.size(); i++) {
                sumOfAverage += averageInvokersUsageRecords.get(i);
                sumOfVmCount += invokersCountList.get(i);
            }
            meanAverageInvokersUsageRecords.add(sumOfAverage / averageInvokersUsageRecords.size());
            meanSumOfInvokersCounts.add(sumOfVmCount / invokersCountList.size());
            averageInvokersUsageRecords.clear();
            invokersCountList.clear();
        }

        send(this.getId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE);
    }

    /**
     * Overrides
     */

    @Override
    protected void processVmCreate(SimEvent ev) {

        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int invokerId = data[1];
        int result = data[2];

        if (result == CloudSimSCTags.TRUE) {

            getVmsToDatacentersMap().put(invokerId, datacenterId); // Core
            getVmsCreatedList().add(ContainerVmList.getById(getVmList(), invokerId));

            invokerIdleList.add(ContainerVmList.getById(getVmList(), invokerId));
            invokerTempTimeMap.put(ContainerVmList.getById(getVmList(), invokerId), new ArrayList<>());
            log.info("{}: {}: New invoker with id: {} was created in the datacenter: {}.",
                    CloudSim.clock(), getName(), invokerId, datacenterId);
            setNumberOfCreatedVMs(getNumberOfCreatedVMs() + 1);
        } else {
            log.error("{}: {}: Creation of new invoker with id: {} in the datacenter: {} failed.",
                    CloudSim.clock(), getName(), invokerId, datacenterId);
        }
        incrementVmsAcks();
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {

        // TODO: Maybe use immutable pairs if I can emit this event.
        Map.Entry<ContainerCloudlet, ContainerVm> data =  (Map.Entry<ContainerCloudlet,ContainerVm>) ev.getData();
        ServerlessRequest request = (ServerlessRequest) data.getKey();
        ServerlessInvoker invoker = (ServerlessInvoker) data.getValue();
        ServerlessContainer container = ContainerList.getById(getContainerList(), request.getContainerId());

        removeFromInvokerRequestsMap(request, invoker);
        removeFromInvokerRequestExecutionMap(request, invoker);

        ServerlessRequestScheduler requestScheduler =
                (ServerlessRequestScheduler) container.getContainerCloudletScheduler();
        requestScheduler.deAllocateResources(request);

        getCloudletReceivedList().add(request);
        ((ServerlessContainer) ContainerList.getById(getContainerList(), request.getContainerId())).removeFromRunningRequests(request);
        ((ServerlessContainer) ContainerList.getById(getContainerList(), request.getContainerId())).addToFinishedRequests(request);

        log.info("{}: {}: request with id: {} returned.",
                CloudSim.clock(), getName(), request.getCloudletId());
        cloudletsSubmitted--;
        tasksReturnedCount++;
    }

    /**
     * Load balancer functionalities
     */

    public void sendFunctionRetryRequest(ServerlessRequest request) {
        send(
                getId(),
                Constants.FUNCTION_SCHEDULING_RETRY_DELAY,
                CloudSimSCTags.CLOUDLET_SUBMIT,
                request
        );
    }

    public void createContainer(ServerlessRequest request, String functionTypeId, int controllerId) {

        ServerlessContainer container =
                new ServerlessContainer(
                        containersMadeCounter++,
                        controllerId,
                        functionTypeId,
                        request.getContainerMIPS(),
                        request.getNumberOfPes(),
                        request.getContainerMemory(),
                        Constants.CONTAINER_BW,
                        Constants.CONTAINER_SIZE,
                        "Xen",
                        new ServerlessRequestScheduler(
                                request.getContainerMIPS(),
                                request.getNumberOfPes()
                        ),
                        Constants.SCHEDULING_INTERVAL
                );
        getContainerList().add(container);
    }

    public void addToFunctionInvokersMap(ServerlessInvoker invoker, String functionId) {

        if (!functionInvokersMap.containsKey(functionId)) {
            List<ServerlessInvoker> invokerListMap = new ArrayList<>();
            invokerListMap.add(invoker);
            functionInvokersMap.put(functionId, invokerListMap);
        } else {
            if (!functionInvokersMap.get(functionId).contains(invoker)) {
                functionInvokersMap.get(functionId).add(invoker);
            }
        }
    }

    public void addToInvokerRequestsMap(ServerlessRequest request, ServerlessInvoker invoker) {
        int count = invoker.getFunctionsMap().getOrDefault(request.getFunctionId(), 0);
        invoker.getFunctionsMap().put(request.getFunctionId(), count + 1);
    }

    public void removeFromInvokerRequestsMap(ServerlessRequest request, ServerlessInvoker invoker) {
        int count = invoker.getFunctionsMap().get(request.getFunctionId());
        invoker.getFunctionsMap().put(request.getFunctionId(), count - 1);
        if (count == 1) {
            functionInvokersMap.get(request.getFunctionId()).remove(invoker);
            if (functionInvokersMap.get(request.getFunctionId()).isEmpty()) {
                functionInvokersMap.remove(request.getFunctionId());
            }
        }
    }

    public void submitRequestToDC(ServerlessRequest request, int invokerId, double delay, int containerId) {

        request.setVmId(invokerId);
        cloudletsSubmitted++;
        getCloudletSubmittedList().add(request);
        getCloudletList().remove(request);

        log.info("{}: {}: Request with id: {} has been submitted to invoker: {} and container: {}.",
                CloudSim.clock(), getName(), request.getCloudletId(), invokerId, containerId);
        if (delay > 0) {
            send(getVmsToDatacentersMap().get(request.getVmId()), delay, CloudSimSCTags.CLOUDLET_SUBMIT_ACK, request);
        } else {
            sendNow(getVmsToDatacentersMap().get(request.getVmId()), CloudSimSCTags.CLOUDLET_SUBMIT_ACK, request);
        }
    }

    public void addToInvokerRequestMap(ServerlessRequest request, ServerlessInvoker invoker) {
        int count = invoker.getRequestMap().getOrDefault(request.getFunctionId(), 0);
        invoker.getRequestMap().put(request.getFunctionId(), count + 1);
    }

    /**
     * Local functionalities
     */

    protected void submitContainer(ServerlessRequest request, ServerlessContainer container) {
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);
    }

    protected void submitRequestToList(ServerlessRequest request) {
        getCloudletList().add(request);
    }

    protected void removeFromInvokerRequestExecutionMap(ServerlessRequest request, ServerlessInvoker invoker) {
        invoker.getRequestExecutionMap().get(request.getFunctionId()).remove(request);
        invoker.getRequestExecutionMapFull().get(request.getFunctionId()).remove(request);
    }

    /**
     * Public functionalities
     */

    public double getAverageResourceUtilization() {
        return MathUtil.mean(meanAverageInvokersUsageRecords);
    }

    public double getAverageInvokersCount() {
        return MathUtil.mean(meanSumOfInvokersCounts);
    }
}
