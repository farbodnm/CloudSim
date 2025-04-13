package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.serverless.components.loadbalance.RequestLoadBalancer;
import org.cloudbus.cloudsim.serverless.utils.CloudSimSCTags;
import org.cloudbus.cloudsim.serverless.utils.Constants;
import org.cloudbus.cloudsim.serverless.utils.IPair;
import org.cloudbus.cloudsim.util.MathUtil;

import java.util.*;

/**
 * Broker class for CloudSimSC extension. This class represents a broker (Service Provider)
 * who uses the Cloud data center.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
@Slf4j
public class ServerlessController extends ContainerDatacenterBroker {

    /**
     * The idling VM list.
     */
    protected List<ServerlessInvoker> vmIdleList = new ArrayList<>();

    protected List<Container> containerList = new ArrayList<>();

    /**
     * The containers destroyed list.
     */
    @Getter
    protected List<? extends ServerlessContainer> containerDestroyedList;

    private static int overbookingFactor = 0;

    /**
     * The arrival time of each serverless request
     */
    public Queue<Double> requestArrivalTime = new LinkedList<>();

    /**
     * The serverless function requests queue
     */
    public Queue<ServerlessRequest> requestQueue = new LinkedList<>();

    /**
     * The task type and vm map of controller - contains the list of vms running each function type
     */
    protected Map<String, ArrayList<ServerlessInvoker>> functionVmMap = new HashMap<>();

    protected List<ServerlessRequest> toSubmitOnContainerCreation = new ArrayList<>();

    protected List<Double> averageVmUsageRecords = new ArrayList<>();

    protected List<Double> meanAverageVmUsageRecords = new ArrayList<>();

    protected List<Integer> vmCountList = new ArrayList<>();

    protected List<Double> meanSumOfVmCount = new ArrayList<>();

    protected double timeInterval = 50.0;

    @Setter
    protected double requestSubmitClock = 0;

    protected Map<ServerlessInvoker, ArrayList<ServerlessRequest>> vmTempTimeMap = new HashMap<>();

    /**
     * The loadBalancer ID.
     */
    @Setter
    private RequestLoadBalancer loadBalancer;

    @Setter
    ServerlessDatacenter datacenter;

    public int containerId = 1;
    public int existingContainerCount = 0;
    public int noOfTasks = 0;
    public int noOfTasksReturned = 0;

    public ServerlessController(String name, int overbookingFactor) throws Exception {
        super(name, overbookingFactor);
        ServerlessController.overbookingFactor = overbookingFactor;
        this.containerDestroyedList = new ArrayList<>();
    }

    @Override
    protected void processOtherEvent(SimEvent ev){
        switch (ev.getTag()) {
            case CloudSimTags.CLOUDLET_SUBMIT:
                submitRequest(ev);
                break;
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processRequestSubmitAck(ev);
                break;
            // other unknown tags are processed by this method
            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev);
                break;
            case CloudSimSCTags.SCALED_CONTAINER:
                processScaledContainer(ev);
                break;
            case CloudSimSCTags.RECORD_CPU_USAGE:
                processRecordCPUUsage();
                break;
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    @Override
    public void startEntity() {
        super.startEntity();
        while(!requestArrivalTime.isEmpty()){
            send(getId(), requestArrivalTime.remove(), CloudSimTags.CLOUDLET_SUBMIT,requestQueue.remove());
        }
    }

    @Override
    public void processContainerCreate(SimEvent ev) {

        int[] data = (int[]) ev.getData();
        int vmId = data[0];
        int containerId = data[1];
        int result = data[2];

        ServerlessContainer cont = Objects.requireNonNull(ContainerList.getById(getContainerList(), containerId));

        if (result == CloudSimTags.TRUE) {
            if (vmId == -1) {
                log.error(
                    "{}: {}: Container: {} was created successfully but no associated invoker was found, container will be stranded",
                    CloudSim.clock(), getName(), containerId);
            } else {
                getContainersToVmsMap().put(containerId, vmId);
                getContainersCreatedList().add(cont);
                cont.setStartTime(CloudSim.clock());
                ServerlessInvoker vm = (ServerlessInvoker) Objects.requireNonNull(ContainerVmList.getById(getVmsCreatedList(), vmId));
                vm.getFunctionContainerMapPending().get(cont.getFunctionType()).remove(cont);
                vm.addToFunctionContainerMap(cont, cont.getFunctionType());
                int hostId =
                    ((ServerlessInvoker) Objects.requireNonNull(ContainerVmList.getById(getVmsCreatedList(), vmId)))
                        .getHost()
                        .getId();
                log.info("{}: {}: Container: {} was created on vm: {} and host: {}",
                    CloudSim.clock(), getName(), containerId, vmId, hostId);
                setContainersCreated(getContainersCreated() + 1);
            }
        } else {
            log.info("{}: {}: Creation of container: {} failed",
                CloudSim.clock(), getName(), containerId);
        }

        incrementContainersAcks();
        List<ServerlessRequest> toRemove = new ArrayList<>();
        if (!toSubmitOnContainerCreation.isEmpty()) {
            for (ServerlessRequest request : toSubmitOnContainerCreation) {
                if (request.getContainerId() == containerId) {
                    ServerlessInvoker vm = (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), vmId));
                    if (vm != null) {
                        addToVmTaskMap(request, vm);
                        vmTempTimeMap.get(vm).remove(request);
                        vm.addToFunctionContainerMap(cont, request.getRequestFunctionId());
                        putInFunctionVmMap(((ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), vmId))), request.getRequestFunctionId());
                        submitRequestToDC(request, vmId, 0, containerId);

                        toRemove.add(request);
                    }
                }
            }
            toSubmitOnContainerCreation.removeAll(toRemove);
            toRemove.clear();
        }
    }

    @Override
    protected void processVmCreate(SimEvent ev) {

        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(ContainerVmList.getById(getVmList(), vmId));

            // Add Vm to idle list
            vmIdleList.add(ContainerVmList.getById(getVmList(), vmId));
            ArrayList<ServerlessRequest> taskList = new ArrayList<>();
            vmTempTimeMap.put(ContainerVmList.getById(getVmList(), vmId), taskList);
            log.info(
                "{}: {}: Invoker {} has been created on host: {} in datacenter: {}",
                CloudSim.clock(),
                getName(),
                vmId,
                ((ServerlessInvoker) Objects.requireNonNull(ContainerVmList.getById(getVmsCreatedList(), vmId)))
                    .getHost()
                    .getId(),
                datacenterId
            );
            setNumberOfCreatedVMs(getNumberOfCreatedVMs() + 1);
        } else {
            log.error("{}: {}: Creation of Invoker {} failed in datacenter {}",
                CloudSim.clock(), getName(), vmId, datacenterId);
        }
        incrementVmsAcks();
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {

        @SuppressWarnings("unchecked")
        IPair<ContainerCloudlet, ContainerVm> data = (IPair<ContainerCloudlet, ContainerVm>) ev.getData();

        ContainerCloudlet request = data.left();
        ContainerVm vm = data.right();
        ServerlessContainer container = Objects.requireNonNull(ContainerList.getById(getContainerList(), request.getContainerId()));

        removeFromVmTaskMap((ServerlessRequest) request, (ServerlessInvoker) vm);
        removeFromVmTaskExecutionMap((ServerlessRequest) request, (ServerlessInvoker) vm);
        ServerlessRequestScheduler clScheduler = (ServerlessRequestScheduler) (container.getContainerCloudletScheduler());
        clScheduler.deAllocateResources((ServerlessRequest) request);

        getCloudletReceivedList().add(request);
        (((ServerlessContainer) Objects.requireNonNull(ContainerList.getById(getContainerList(), request.getContainerId())))
            .getRunningTasks()).remove(request);
        ((ServerlessContainer) Objects.requireNonNull(ContainerList.getById(getContainerList(), request.getContainerId())))
            .addToFinishedTasks((ServerlessRequest) request);

        log.info("{}: {}: request {} returned",
            CloudSim.clock(), getName(), request.getCloudletId());

        cloudletsSubmitted--;
        noOfTasksReturned++;
    }

    /**
     * Event processors
     */

    public void submitRequest(SimEvent ev) {
        ServerlessRequest cl = (ServerlessRequest) ev.getData();
        log.info("{}: {}: Request: {} arrived",
            CloudSim.clock(), getName(), cl.getCloudletId());
        if (CloudSim.clock() == requestSubmitClock) {
            send(getId(), Constants.MINIMUM_INTERVAL_BETWEEN_TWO_CLOUDLET_SUBMISSIONS, CloudSimTags.CLOUDLET_SUBMIT, cl);
        } else {
            datacenter.updateCloudletProcessing();
            addToCloudlets(cl);
            loadBalancer.routeRequest(cl);
        }
    }

    protected void processScaledContainer(SimEvent ev) {

        String[] data = (String[]) ev.getData();
        int brokerId = Integer.parseInt(data[0]);
        String requestId = data[1];
        double containerMips = Double.parseDouble(data[2]);
        int containerRAM = (int) Double.parseDouble(data[3]);
        int containerPES = (int) Double.parseDouble(data[4]);

        ServerlessContainer container =
            new ServerlessContainer(
                containerId,
                brokerId,
                requestId,
                containerMips,
                containerPES,
                containerRAM,
                Constants.CONTAINER_BW,
                Constants.CONTAINER_SIZE,
                "Xen",
                new ServerlessRequestScheduler(
                    containerMips,
                    containerPES
                ),
                Constants.SCHEDULING_INTERVAL,
                true,
                false,
                false,
                0
            );
        getContainerList().add(container);
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);
        containerId++;
        log.info("{}: {}: Processed and created the now scaled container: {}",
            CloudSim.clock(), getName(), container.getId());
    }

    public void processRequestSubmitAck(SimEvent ev) {

        ServerlessRequest task = (ServerlessRequest) ev.getData();
        (
            (ServerlessContainer) Objects.requireNonNull(
                ContainerList.getById(getContainersCreatedList(), task.getContainerId())
            )
        ).newContainer = false;
    }

    public void processContainerDestroy(SimEvent ev) {

        int[] data = (int[]) ev.getData();
        // data[0] (datacenter id) ignored
        int containerId = data[1];
        int result = data[2];
        int oldVmId = data[3];

        log.debug("{}: {}: Container {} is destroyed",
            CloudSim.clock(), getName(), containerId);

        if (result == CloudSimTags.TRUE) {

            // If no more containers, add vm to idle list
            if (
                ((ServerlessInvoker) Objects.requireNonNull(ContainerVmList.getById(getVmsCreatedList(), oldVmId)))
                    .getContainerList().isEmpty()
            ) {
                vmIdleList.add((ContainerVmList.getById(getVmsCreatedList(), oldVmId)));
            }

            getContainersToVmsMap().remove(containerId);
            containersCreatedList.remove(ContainerList.getById(getContainersCreatedList(), containerId));
            containerDestroyedList.add(ContainerList.getById(getContainerList(), containerId));
            ((ServerlessContainer) Objects.requireNonNull(ContainerList.getById(getContainerList(), containerId)))
                .setFinishTime(CloudSim.clock());
            setContainersCreated(getContainersCreated() - 1);
        } else {
            log.error("{}: {}: Failed to destroy container {}",
                CloudSim.clock(), getName(), containerId);
        }
    }

    private void processRecordCPUUsage() {

        double utilization;
        int vmCount = 0;
        double sum = 0;

        for (int x = 0; x < getVmsCreatedList().size(); x++) {
            utilization = 1 - getVmsCreatedList().get(x).getAvailableMips() / getVmsCreatedList().get(x).getTotalMips();
            if (utilization > 0) {
                ((ServerlessInvoker) getVmsCreatedList().get(x)).used = true;
                vmCount++;
                sum += utilization;
            }
        }

        if (sum > 0) {
            averageVmUsageRecords.add(sum / vmCount);
            vmCountList.add(vmCount);
        }

        double sumOfAverage = 0;
        double sumOfVmCount = 0;
        if (averageVmUsageRecords.size() == Constants.CPU_HISTORY_LENGTH) {
            for (int x = 0; x < Constants.CPU_HISTORY_LENGTH; x++) {
                sumOfAverage += averageVmUsageRecords.get(x);
                sumOfVmCount += vmCountList.get(x);
            }
            meanAverageVmUsageRecords.add(sumOfAverage / Constants.CPU_HISTORY_LENGTH);
            meanSumOfVmCount.add(sumOfVmCount / Constants.CPU_HISTORY_LENGTH);
            averageVmUsageRecords.clear();
            vmCountList.clear();
        }

        send(this.getId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE);
    }

    /**
     * Global functionalities
     */

    public void addToVmTaskMap(ServerlessRequest task, ServerlessInvoker vm) {

        int count = vm.getVmTaskMap().getOrDefault(task.getRequestFunctionId(), 0);
        vm.getVmTaskMap().put(task.getRequestFunctionId(), count + 1);
    }

    public void submitRequestToDC(ServerlessRequest request, int vmId, double delay, int containerId){

        request.setVmId(vmId);
        cloudletsSubmitted++;
        getCloudletSubmittedList().add(request);
        getCloudletList().remove(request);

        log.info("{}: {}: Request {} has been submitted to invoker invoker {} in container {}",
            CloudSim.clock(), getName(), request.getCloudletId(), vmId, containerId);

        if (delay > 0) {
            send(getVmsToDatacentersMap().get(request.getVmId()), delay, CloudSimTags.CLOUDLET_SUBMIT_ACK, request);
        } else {
            sendNow(getVmsToDatacentersMap().get(request.getVmId()), CloudSimTags.CLOUDLET_SUBMIT_ACK, request);
        }
    }

    public void putInFunctionVmMap(ServerlessInvoker vm, String functionId) {

        if (!functionVmMap.containsKey(functionId)) {
            ArrayList<ServerlessInvoker> vmListMap = new ArrayList<>();
            vmListMap.add(vm);
            functionVmMap.put(functionId, vmListMap);
        } else {
            if (!functionVmMap.get(functionId).contains(vm)) {
                functionVmMap.get(functionId).add(vm);
            }
        }
    }

    /**
     * Load balancer specific functionalities
     */

    public void sendFunctionRetryRequest(ServerlessRequest req) {
        send(getId(), Constants.FUNCTION_SCHEDULING_RETRY_DELAY, CloudSimTags.CLOUDLET_SUBMIT, req);
    }

    public void createContainer(ServerlessRequest cl, String requestId, int brokerId) {

        ServerlessContainer container =
            new ServerlessContainer(
                containerId,
                brokerId,
                requestId,
                cl.getContainerMIPS(),
                cl.getNumberOfPes(),
                cl.getContainerMemory(),
                Constants.CONTAINER_BW,
                Constants.CONTAINER_SIZE,
                "Xen",
                new ServerlessRequestScheduler(
                    cl.getContainerMIPS(),
                    cl.getNumberOfPes()
                ),
                Constants.SCHEDULING_INTERVAL,
                true,
                false,
                false,
                0
            );
        getContainerList().add(container);
        cl.setContainerId(containerId);
        submitContainer(container);
        containerId++;
    }

    public void addToSubmitOnContainerCreation(ServerlessRequest request) {
        toSubmitOnContainerCreation.add(request);
    }

    /**
     * Local functionalities
     */

    private void submitContainer(Container container) {
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);
    }


    private void removeFromVmTaskMap(ServerlessRequest task, ServerlessInvoker vm) {

        int count = vm.getVmTaskMap().get(task.getRequestFunctionId());
        vm.getVmTaskMap().put(task.getRequestFunctionId(), count - 1);
        if (count == 1) {
            functionVmMap.get(task.getRequestFunctionId()).remove(vm);
            if (functionVmMap.get(task.getRequestFunctionId()) == null) {
                functionVmMap.remove(task.getRequestFunctionId());
            }
        }
    }

    private void removeFromVmTaskExecutionMap(ServerlessRequest task, ServerlessInvoker vm) {

        vm.getVmTaskExecutionMap().get(task.getRequestFunctionId()).remove(task);
        vm.getVmTaskExecutionMapFull().get(task.getRequestFunctionId()).remove(task);
    }

    private void addToCloudlets(ServerlessRequest request) {
        getCloudletList().add(request);
    }

    /**
     * Report generation functionalities
     */

    public double getAverageResourceUtilization(){

        double sumAverage=0;

        for (Double meanAverageVmUsageRecord : meanAverageVmUsageRecords) {
            sumAverage += meanAverageVmUsageRecord;
        }
        return (sumAverage / meanAverageVmUsageRecords.size());
    }

    public double getAverageVmCount (){
        return MathUtil.mean(meanSumOfVmCount);
    }
}
