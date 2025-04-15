package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.serverless.components.autoscale.FunctionAutoScaler;
import org.cloudbus.cloudsim.serverless.components.schedule.FunctionScheduler;
import org.cloudbus.cloudsim.serverless.enums.AutoScalerType;
import org.cloudbus.cloudsim.serverless.enums.InvokerStatus;
import org.cloudbus.cloudsim.serverless.utils.CloudSimSCTags;
import org.cloudbus.cloudsim.serverless.utils.Constants;
import org.cloudbus.cloudsim.serverless.utils.IPair;

import java.util.*;

/**
 * Datacenter class for CloudSimSC extension. This class represents the physical infrastructure of the service provider
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
@Slf4j
public class ServerlessDatacenter extends PowerContainerDatacenterCM {

    /**
     * Idle Vm list
     */
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final static List<ServerlessInvoker> vmIdleList = new ArrayList<>();

    /**
     * The longest reaming run time of the requests running on each vm
     */
    @Getter
    private final static Map<Integer, Double> runTimeVm = new HashMap<>();

    /**
     * Perform Datacenter monitoring
     */
    private final boolean monitoring;

    /**
     * Idle containers that need to be destroyed
     */
    @Getter
    private final List<Container> containersToDestroy = new ArrayList<>();

    /**
     * request submit event of DC event
     */
    protected boolean autoScalingInitialized = false;

    private final FunctionAutoScaler autoScaler;

    public ServerlessDatacenter(
        String name,
        ContainerDatacenterCharacteristics characteristics,
        ContainerVmAllocationPolicy vmAllocationPolicy,
        FunctionScheduler containerAllocationPolicy,
        List<Storage> storageList,
        double schedulingInterval,
        String experimentName,
        String logAddress,
        double vmStartupDelay,
        double containerStartupDelay,
        boolean monitor,
        AutoScalerType autoScalerType
    ) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
        this.monitoring = monitor;
        autoScaler = Objects.requireNonNull(autoScalerType.getAutoScalerConstructor()).newInstance(this);
    }

    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.CONTAINER_DESTROY:
                processContainerDestroy(ev, false);
                break;

            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev, true);
                break;

            case containerCloudSimTags.CONTAINER_SUBMIT_FOR_RESCHEDULE:
                ((ServerlessContainer) ev.getData()).setReschedule(true);
                processContainerSubmit(ev, true);
                break;

            case CloudSimSCTags.AUTO_SCALE:
                processAutoScaling();
                break;

            // other unknown tags are processed by this method
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    @Override
    public void updateCloudletProcessing() {

        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
        if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + CloudSim.getMinTimeBetweenEvents() / 1000) {
            List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
            double smallerTime = Double.MAX_VALUE;
            for (ContainerHost host : list) {
                // inform VMs to update processing
                double time = host.updateContainerVmsProcessing(CloudSim.clock());
                // what time do we expect that the next request will finish?
                if (time < smallerTime) {
                    smallerTime = time;
                }
            }

            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(CloudSim.clock());

            // Destroy idling containers if this is a DC_EVENT
            if (!Constants.FUNCTION_AUTOSCALING) {
                destroyIdleContainers();
            }
        }
    }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {

        updateCloudletProcessing();
        try {
            ServerlessRequest cl = (ServerlessRequest) ev.getData();

            // TODO: Cast will fail if the this happens I think?
            // checks whether this request has finished or not
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                log.warn("{}: {}: Request {} owned by {} was already finished by the time it reached the datacenter",
                    CloudSim.clock(), getName(), cl.getCloudletId(), name);

                // NOTE: If a request has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this request back.
//                if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();

                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                return;
            }

            // process this request to this CloudResource
            cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
                .getCostPerBw(), cl.getVmId());

            int userId = cl.getUserId();
            int vmId = cl.getVmId();
            int containerId = cl.getContainerId();

            // time to transfer the files
            double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
            ContainerHost host;

            host = getVmAllocationPolicy().getHost(vmId, userId);
            ServerlessInvoker vm;
            Container container;
            double estimatedFinishTime;

            vm = (ServerlessInvoker) host.getContainerVm(vmId, userId);
            container = vm.getContainer(containerId, userId);


            estimatedFinishTime =
                ((ServerlessRequestScheduler) container.getContainerCloudletScheduler())
                    .requestSubmit(cl, vm);
            log.info("{}: {}: Estimated finish time of function {} before submittion is {}ms",
                CloudSim.clock(), getName(), cl.getCloudletId(), estimatedFinishTime);

            // Remove the new request's container from removal list
            containersToDestroy.remove(container);
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;
                send(getId(), (estimatedFinishTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
            }
            int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
            sendNow(cl.getUserId(), tag, cl);
        } catch (ClassCastException c) {
            log.error("{}: {}: An unexpected cast exception occurred {}",
                CloudSim.clock(), getName(), c.getStackTrace());
        } catch (Exception e) {
            log.error("{}: {}: An unexpected error occurred {}",
                CloudSim.clock(), getName(), e.getStackTrace());
        }

        checkCloudletCompletion();
        // TODO: Keeping all containers alive after they finish causes new request to be unable to get processed.
        if (Constants.FUNCTION_AUTOSCALING && !autoScalingInitialized) {
            autoScalingInitialized = true;
            autoScaler.scaleFunctions();
            destroyIdleContainers();
            send(this.getId(), Constants.AUTO_SCALING_INTERVAL, CloudSimSCTags.AUTO_SCALE);
        }
    }

    @Override
    protected void checkCloudletCompletion() {
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
            for (ContainerVm vm : host.getVmList()) {
                for (Container container : vm.getContainerList()) {
                    while (container.getContainerCloudletScheduler().isFinishedCloudlets()) {
                        Cloudlet cl = container.getContainerCloudletScheduler().getNextFinishedCloudlet();
                        if (cl != null) {
                            IPair<Cloudlet, ContainerVm> data = IPair.of(cl, vm);
                            ((ServerlessInvoker) vm).getRunningRequestList().removeIf((request -> request == cl));
                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, data);
                        }
                    }
                }
            }
        }
    }

    /**
     * Event processors
     */

    public void processAutoScaling() {

        log.info("{}: {}: Processing autoscaling request.",
            CloudSim.clock(), getName());
        autoScaler.scaleFunctions();
        destroyIdleContainers();
        send(this.getId(), Constants.AUTO_SCALING_INTERVAL, CloudSimSCTags.AUTO_SCALE);
    }

    public void processContainerDestroy(SimEvent ev, boolean ack) {

        Container container = (Container) ev.getData();
        if (Constants.CONTAINER_IDLING_ENABLED) {
            if (
                Math.round(CloudSim.clock() * 100000) / 100000
                    - Math.round(((ServerlessContainer) container).getIdleStartTime() * 100000) / 100000
                    >= Constants.CONTAINER_IDLING_TIME
            ) {
                log.info("{}: {}: Destroying idle container: {}", CloudSim.clock(), getName(), container.getId());
                destroyIdleContainer(ack, container);
            }
        } else {
            destroyIdleContainer(ack, container);
        }
    }

    @Override
    protected void processVmCreate(SimEvent ev, boolean ack) {

        ContainerVm containerVm = (ContainerVm) ev.getData();
        log.info("{}: {}: Started processing invoker {} creation event",
            CloudSim.clock(), getName(), containerVm.getId());

        boolean result = getVmAllocationPolicy().allocateHostForVm(containerVm);
        if (ack) {
            if (result) {
                sendAck(
                    containerVm.getUserId(),
                    CloudSimTags.VM_CREATE_ACK,
                    CloudSim.getMinTimeBetweenEvents(),
                    getId(), containerVm.getId(), CloudSimTags.TRUE
                );
            } else {
                sendAck(
                    containerVm.getUserId(),
                    CloudSimTags.VM_CREATE_ACK,
                    CloudSim.getMinTimeBetweenEvents(),
                    getId(), containerVm.getId(), CloudSimTags.FALSE
                );
            }
            // Add Vm to idle list
            vmIdleList.add((ServerlessInvoker) containerVm);
            ((ServerlessInvoker) containerVm).setStatus(InvokerStatus.OFF);
            ((ServerlessInvoker) containerVm).setRecordTime(CloudSim.clock());
        }

        runTimeVm.put(containerVm.getId(), (double) 0);
        if (result) {
            getContainerVmList().add((PowerContainerVm) containerVm);

            if (containerVm.isBeingInstantiated()) {
                containerVm.setBeingInstantiated(false);
            }

            containerVm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(containerVm).getContainerVmScheduler()
                .getAllocatedMipsForContainerVm(containerVm));

            if (Constants.MONITORING) {
                send(containerVm.getUserId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE, containerVm);
            }
        }

    }

    @Override
    public void processContainerSubmit(SimEvent ev, boolean ack) {

        Container container = (Container) ev.getData();
        boolean result;

        // Directly allocate resources to container if a vm is assigned already
        if (container.getVm() != null) {
            result = ((FunctionScheduler) getContainerAllocationPolicy()).allocateVmForContainer(container, container.getVm(), getContainerVmList());
        } else {
            result = ((FunctionScheduler) getContainerAllocationPolicy()).allocateVmForContainer(container, getContainerVmList());
        }

        if (ack) {
            if (result) {
                ServerlessInvoker vm = ((ServerlessInvoker) container.getVm());
                if (monitoring) {
                    // Remove vm from idle list when first container is created
                    if (vm.getContainerList().size() == 1) {
                        vmIdleList.remove(vm);
                        vm.setStatus(InvokerStatus.ON);
                        vm.offTime += (CloudSim.clock() - vm.getRecordTime());
                        vm.setRecordTime(CloudSim.clock());
                    }
                }
                if (vm.getId() == -1) {
                    log.warn("{}: {}: Container {} was submitted successfully but the vm ID is unknown (-1), container will be stranded.",
                        CloudSim.clock(), getName(), container.getId());
                }
                getContainerList().add(container);
                if (container.isBeingInstantiated()) {
                    container.setBeingInstantiated(false);
                }

                ((ServerlessContainer) container).updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerVm(container).getContainerScheduler().getAllocatedMipsForContainer(container), vm);
                vm.addToFunctionContainerMapPending(container, ((ServerlessContainer) container).getFunctionType());
                sendAck(
                    ev.getSource(),
                    containerCloudSimTags.CONTAINER_CREATE_ACK,
                    Constants.CONTAINER_STARTUP_DELAY,
                    vm.getId(), container.getId(), CloudSimTags.TRUE, 0
                );
            } else {
                log.error("{}: {}: No invoker was found to host the container {}",
                    CloudSim.clock(), getName(), container.getId());
            }
        }
    }

    /**
     * Auto-scaler functionalities
     */

    public void containerVerticalScale(Container container, ServerlessInvoker vm, int cpuChange, int memChange) {
        ((FunctionScheduler) getContainerAllocationPolicy())
            .reallocateVmResourcesForContainer(container, vm, cpuChange, memChange);
    }

    public void sendScaledContainerCreationRequest(String[] data) {
        sendNow(Integer.parseInt(data[0]), CloudSimSCTags.SCALED_CONTAINER, data);
    }

    /**
     * Local functionalities
     */

    public void sendAck(int target, int eventTag, Double interval, int... data) {

        if (interval != null) {
            send(target, interval, eventTag, data);
        } else {
            sendNow(target, eventTag, data);
        }
    }

    private void destroyIdleContainer(boolean ack, Container container) {

        ServerlessInvoker vm = (ServerlessInvoker) container.getVm();
        if (vm != null) {
            getContainerAllocationPolicy().deallocateVmForContainer(container);

            // Add vm to idle list if there are no more containers
            if ((vm.getContainerList()).isEmpty()) {
                vmIdleList.add((ServerlessInvoker) container.getVm());
                if (vm.getStatus().equals(InvokerStatus.ON)) {
                    vm.setStatus(InvokerStatus.OFF);
                    vm.onTime += (CloudSim.clock() - vm.getRecordTime());
                } else if (vm.getStatus().equals(InvokerStatus.OFF)) {
                    vm.offTime += (CloudSim.clock() - vm.getRecordTime());
                }
                vm.setRecordTime(CloudSim.clock());
            }
            if (ack) {
                sendAck(
                    container.getUserId(),
                    CloudSimTags.CONTAINER_DESTROY_ACK,
                    null,
                    getId(), container.getId(), CloudSimTags.TRUE, vm.getId()
                );
            }
            getContainerList().remove(container);
        }
    }

    private void destroyIdleContainers() {
        while (!containersToDestroy.isEmpty()) {
            for (Container container : containersToDestroy) {
                if (((ServerlessContainer) container).newContainer) {
                    ((ServerlessContainer) container).setIdleStartTime(0);
                    continue;
                }
                if (!Constants.CONTAINER_IDLING_ENABLED) {
                    sendNow(getId(), CloudSimTags.CONTAINER_DESTROY_ACK, container);
                } else {
                    send(getId(), Constants.CONTAINER_IDLING_TIME, CloudSimTags.CONTAINER_DESTROY_ACK, container);
                }
            }
            containersToDestroy.clear();
        }
    }

    public void updateRunTimeVm(ServerlessInvoker vm, double time) {
        if (runTimeVm.get(vm.getId()) < time) {
            runTimeVm.put(vm.getId(), time);
        }
    }
}


