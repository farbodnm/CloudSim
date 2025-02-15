package org.cloudbus.cloudsim.serverless.components.process;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.serverless.components.scaling.FunctionAutoScaler;
import org.cloudbus.cloudsim.serverless.components.scheduling.FunctionScheduler;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.enums.InvokerStatus;
import org.cloudbus.cloudsim.serverless.utils.CloudSimSCTags;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.*;


/**
 * Datacenter class for CloudSimSC extension. This class represents the physical infrastructure of the service provider
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

@Getter
@Setter
@Slf4j
public class ServerlessDatacenter extends PowerContainerDatacenterCM {

    public static Map<Integer, Double> invokerRunTimes = new HashMap<>();
    public static List<ServerlessInvoker> idleInvokers = new ArrayList<>();

    private List<Container> containersToDestroy = new ArrayList<>();
    private FunctionAutoScaler autoScaler;

    private boolean isMonitored = false;
    private boolean isAutoScalingInitialized = false;

    public ServerlessDatacenter(
            String name,
            ContainerDatacenterCharacteristics characteristics,
            ContainerVmAllocationPolicy vmAllocationPolicy,
            ContainerAllocationPolicy containerAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval,
            String experimentName,
            String logAddress,
            double vmStartupDelay,
            double containerStartupDelay,
            boolean isMonitored,
            FunctionAutoScaler functionAutoScaler
    ) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
        this.isMonitored = isMonitored;
        this.autoScaler = functionAutoScaler;
    }

    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimSCTags.CONTAINER_DESTROY:
                processContainerDestroy(ev, false);
                break;
            case CloudSimSCTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev, true);
                break;
            case containerCloudSimTags.CONTAINER_SUBMIT_FOR_RESCHEDULE:
                ((ServerlessContainer) ev.getData()).setReschedule(true);
                processContainerSubmit(ev, true);
                break;
            case CloudSimSCTags.AUTO_SCALE:
                processAutoScaling(ev);
                break;
        }
    }

    /**
     * Event handlers and overrides
     */

    @Override
    protected void processVmCreate(SimEvent ev, boolean ack) {
        ServerlessInvoker invoker = (ServerlessInvoker) ev.getData();
        boolean result = getVmAllocationPolicy().allocateHostForVm(invoker);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = invoker.getId();
            data[2] = result ? CloudSimSCTags.TRUE : CloudSimSCTags.FALSE;
            send(invoker.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimSCTags.VM_CREATE_ACK, data);

            idleInvokers.add(invoker);
            invoker.setStatus(InvokerStatus.OFF);
            invoker.setRecordTime(CloudSim.clock());
        }

        invokerRunTimes.put(invoker.getId(), 0D);
        if (result) {
            // Add the invoker to the core datacenter VMs list
            getContainerVmList().add(invoker);
            invoker.setBeingInstantiated(false);
            invoker.updateVmProcessing(
                    CloudSim.clock(),
                    getVmAllocationPolicy()
                            .getHost(invoker)
                            .getContainerVmScheduler()
                            .getAllocatedMipsForContainerVm(invoker)
            );

            if (Constants.MONITORING) {
                send(invoker.getUserId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE, invoker);
            }
        }
    }

    @Override
    public void processContainerSubmit(SimEvent ev, boolean ack) {

        ServerlessContainer container = (ServerlessContainer) ev.getData();
        log.info("{}: {}: Processing new submitted container: {}",
                CloudSim.clock(), getName(), container.getId());
        boolean result;
        // TODO: Review after writing the function scheduler
        if (container.getVm() != null) {
            result = getContainerAllocationPolicy().allocateVmForContainer(container, container.getVm(), getContainerVmList());
        } else {
            result = getContainerAllocationPolicy().allocateVmForContainer(container, getContainerVmList());
        }

        if (ack) {
            int[] data = new int[4];
            if (result) {
                ServerlessInvoker invoker = (ServerlessInvoker) container.getVm();
                if (isMonitored) {
                    if (invoker.getContainerList().size() == 1) {
                        idleInvokers.remove(invoker);
                        invoker.setStatus(InvokerStatus.ON);
                        invoker.offTime += CloudSim.clock() - invoker.getRecordTime();
                        invoker.setRecordTime(CloudSim.clock());
                    }
                }
                data[0] = invoker.getId();
                data[1] = container.getId();
                data[2] = CloudSimSCTags.TRUE;
                data[3] = 0;

                // Add the container to the core
                getContainerList().add(container);
                container.setBeingInstantiated(false);
                container.updateContainerProcessing(
                        CloudSim.clock(),
                        getContainerAllocationPolicy()
                                .getContainerVm(container)
                                .getContainerScheduler()
                                .getAllocatedMipsForContainer(container),
                        invoker
                );
                invoker.addToPendingFunctionContainerMap(container, container.getFunctionType());
                send(ev.getSource(), Constants.CONTAINER_STARTUP_DELAY, containerCloudSimTags.CONTAINER_CREATE_ACK, data);
            } else {
                log.error("{}: {}: Couldn't find an invoker to host the container: {}",
                        CloudSim.clock(), getName(), container.getId());
            }
        }
    }

    private void processAutoScaling(SimEvent ev) {
        throw new UnsupportedOperationException("Processing auto scale events not implemented.");
    }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {

        updateCloudletProcessing();
        try {
            ServerlessRequest request = (ServerlessRequest) ev.getData();
            if (request.isFinished()) {
                log.warn("{}: {}: Request: {} owned by: {} with the name: {} was already finished before being submitted to datacenter",
                        CloudSim.clock(), getName(), request.getCloudletId(), CloudSim.getEntityName(request.getUserId()), getName());
                int[] data = new int[3];
                data[0] = getId();
                data[1] = request.getCloudletId();
                sendNow(request.getUserId(), CloudSimSCTags.CLOUDLET_SUBMIT_ACK, data);
                sendNow(request.getUserId(), CloudSimSCTags.CLOUDLET_RETURN, request);
            } else {
                request.setResourceParameter(
                        getId(),
                        getCharacteristics().getCostPerSecond(),
                        getCharacteristics().getCostPerBw(),
                        request.getVmId()
                );

                int userId = request.getUserId();
                int invokerId = request.getVmId();
                int containerId = request.getContainerId();
                double fileTransferTime = predictFileTransferTime(request.getRequiredFiles());

                // TODO: Maybe we have to change the allocation policy to enable same user invokers?
                ContainerHost host = getVmAllocationPolicy().getHost(invokerId, userId);
                ServerlessInvoker invoker = (ServerlessInvoker) host.getContainerVm(invokerId, userId);
                ServerlessContainer container = (ServerlessContainer) invoker.getContainer(containerId, userId);
                ServerlessRequestScheduler scheduler = (ServerlessRequestScheduler) container.getContainerCloudletScheduler();
                double estimatedFinishTime = scheduler.requestSubmit(request, invoker, container);

                containersToDestroy.remove(container);

                if (estimatedFinishTime > 0.0 && Double.isFinite(estimatedFinishTime)) {
                    estimatedFinishTime += fileTransferTime;
                    send(getId(), estimatedFinishTime - CloudSim.clock(), CloudSimSCTags.VM_DATACENTER_EVENT);
                }
                sendNow(request.getUserId(), CloudSimSCTags.CLOUDLET_SUBMIT_ACK, request);
            }
        } catch (ClassCastException exception) {
            log.error("{}: {}: Class cast exception occurred when submitting new request, with message: {}",
                    CloudSim.clock(), getName(), exception.getMessage());
        } catch (Exception exception) {
            log.error("{}: {}: Unexpected exception occurred when submitting new request, with message: {}",
                    CloudSim.clock(), getName(), exception.getMessage());
        }

        checkCloudletCompletion();
        if (!isAutoScalingInitialized) {
            isAutoScalingInitialized = true;
            autoScaler.scaleFunctions();
            destroyIdleContainers();
            send(this.getId(), Constants.AUTO_SCALING_INTERVAL, CloudSimSCTags.AUTO_SCALE);
        }
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
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimSCTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(CloudSim.clock());
        }
    }

    @Override
    protected void checkCloudletCompletion() {

        List<ContainerHost> containerHosts = getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost containerHost: containerHosts) {
            for (ContainerVm invoker: containerHost.getVmList()) {
                for (Container container: invoker.getContainerList()) {
                    while (container.getContainerCloudletScheduler().isFinishedCloudlets()) {
                        Cloudlet request =
                                container.getContainerCloudletScheduler().getNextFinishedCloudlet();
                        if (request != null) {
                            Map.Entry<Cloudlet, ContainerVm> data = new AbstractMap.SimpleEntry<>(request, invoker);
                            for (int i = 0; i < ((ServerlessInvoker) invoker).getRunningRequestsList().size(); i++) {
                                if (((ServerlessInvoker) invoker).getRunningRequestsList().get(i).equals(request)) {
                                    ((ServerlessInvoker) invoker).getRunningRequestsList().remove(i);
                                }
                            }
                            sendNow(request.getUserId(), CloudSimSCTags.CLOUDLET_RETURN, data);
                        }
                    }
                }
            }
        }
    }

    private void processContainerDestroy(SimEvent ev, boolean ack) {

        ServerlessContainer container = (ServerlessContainer) ev.getData();
        if (CloudSim.clock() - container.getIdleStartTime() >= Constants.CONTAINER_IDLING_TIME) {
            ServerlessInvoker invoker = (ServerlessInvoker) container.getVm();
            if (invoker != null) {
                getContainerAllocationPolicy().deallocateVmForContainer(container);
                if (invoker.getContainerList().isEmpty()) {
                    idleInvokers.add(invoker);
                    if (invoker.getStatus().equals(InvokerStatus.ON)) {
                        invoker.setStatus(InvokerStatus.OFF);
                        invoker.onTime += CloudSim.clock() - invoker.getRecordTime();
                    } else if (invoker.getStatus().equals(InvokerStatus.OFF)) {
                        invoker.offTime += CloudSim.clock() - invoker.getRecordTime();
                    }
                }
                if (ack) {
                    int[] data = new int[4];
                    data[0] = getId();
                    data[1] = container.getId();
                    data[2] = CloudSimSCTags.TRUE;
                    data[3] = invoker.getId();
                    sendNow(container.getUserId(), CloudSimSCTags.CONTAINER_DESTROY_ACK, data);
                }
            }
        }
    }

    protected void destroyIdleContainers() {
        for (Container container: containersToDestroy) {
            ServerlessContainer serverlessContainer = (ServerlessContainer) container;
            if ((serverlessContainer.isFirstProcess())) {
                serverlessContainer.setIdleStartTime(0);
            } else {
                send(getId(), Constants.CONTAINER_IDLING_TIME, CloudSimSCTags.CONTAINER_DESTROY_ACK, serverlessContainer);
            }
        }
        containersToDestroy.clear();
    }

    /**
     * Auto scaler functionalities
     */

    public void sendScaledContainerCreationRequest(String[] data) {
        sendNow(Integer.parseInt(data[0]), CloudSimSCTags.SCALED_CONTAINER, data);
    }

    public void containerVerticalScale(ServerlessContainer container, ServerlessInvoker invoker, int cpuChange, int memChange) {
        boolean result = ((FunctionScheduler) getContainerAllocationPolicy()).reallocateVmResourcesForContainer(container, invoker, cpuChange, memChange);
    }
}
