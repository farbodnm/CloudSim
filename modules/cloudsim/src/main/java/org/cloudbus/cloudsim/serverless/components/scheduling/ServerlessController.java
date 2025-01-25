package org.cloudbus.cloudsim.serverless.components.scheduling;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterBroker;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.utils.CloudSimSCTags;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.*;

@Slf4j
@Getter
@Setter
public class ServerlessController extends ContainerDatacenterBroker {

    protected List<ServerlessContainer> containerDestroyedList;
    protected List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    protected double requestSubmitClock = 0;
    protected ServerlessDatacenter datacenter;
    protected List<Double> averageVmUsageRecords = new ArrayList<>();
    protected List<Integer> vmCountList = new ArrayList<>();
    protected List<Double> meanAverageVmUsageRecords = new ArrayList<>();
    protected List<Double> meanSumOfVmCounts = new ArrayList<>();

    public Queue<Double> requestArrivalTimes = new LinkedList<>();
    public Queue<ServerlessRequest> requestQueue = new LinkedList<>();

    private RequestLoadBalancer loadBalancer;

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
            case CloudSimSCTags.RECORD_CPU_USAGE:
                recordResourceUtilization(ev);
                break;
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    private void submitRequest(SimEvent ev) {
        ServerlessRequest request = (ServerlessRequest) ev.getData();
        log.info("Request: {} arrived at: {}", request.getCloudletId(), CloudSim.clock());
        if (CloudSim.clock() == requestSubmitClock) {
            send(
                    getId(),
                    Constants.MINIMUM_INTERVAL_BETWEEN_TWO_CLOUDLET_SUBMISSIONS,
                    CloudSimSCTags.CLOUDLET_SUBMIT,
                    request
            );
        }
        else {
            datacenter.updateCloudletProcessing();
            getCloudletList().add(request);
            loadBalancer.routeRequest(request);
        }
    }

    private void processRequestSubmitAck(SimEvent ev) {
        ServerlessRequest request = (ServerlessRequest) ev.getData();
        ServerlessContainer container = ContainerList.getById(getContainersCreatedList(), request.getContainerId());
        if (container != null) {
            container.setFirstProcess(false);
        }
    }

    private void processContainerDestroy(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        // data[0](DC ID) Ignored since we have one dc.
        int containerId = data[1];
        int result = data[2];
        int vmId = data[3];
        ServerlessContainer container = ContainerList.getById(getContainersCreatedList(), containerId);
        try {
            if (Objects.equals(result, CloudSimSCTags.TRUE)) {
                log.info("Container: {} was destroyed at: {}", containerId, CloudSim.clock());
                ServerlessInvoker invoker = (ContainerVmList.getById(getVmsCreatedList(), vmId));
                assert invoker != null;
                if (invoker.getContainerList().isEmpty()) {
                    vmIdleList.add(invoker);
                }
                getContainersToVmsMap().remove(containerId);
                getContainersCreatedList().remove(container);
                containerDestroyedList.add(container);
                assert container != null;
                container.setFinishTime(CloudSim.clock());
                setContainersCreated(getContainersCreated() - 1);
            } else {
                throw new RuntimeException("Container failed to be destroyed");
            }
        } catch (Exception e) {
            log.error("Something went wrong with the destruction of container: {} at: {} with message: {}",
                    containerId,
                    CloudSim.clock(),
                    e.getMessage()
            );
        }
    }

    private void recordResourceUtilization(SimEvent ignored) {

        double utilization;
        int vmCount = 0;
        double utilizationSum = 0;
        for (ContainerVm vm: vmsCreatedList) {
            ServerlessInvoker invoker = (ServerlessInvoker) vm;
            utilization = 1 - invoker.getAvailableMips() / invoker.getTotalMips();
            if (utilization > 0) {
                invoker.setUsed(true);
                vmCount++;
                utilizationSum += utilization;
            }
        }

        if (utilizationSum > 0) {
            averageVmUsageRecords.add(utilizationSum / vmCount);
            vmCountList.add(vmCount);
        }

        if (averageVmUsageRecords.size() >= Constants.CPU_HISTORY_LENGTH) {
            double sumOfAverages = 0;
            double sumOfVmCounts = 0;
            for (int i = 0; i < averageVmUsageRecords.size(); i++) {
                sumOfAverages += averageVmUsageRecords.get(i);
                sumOfVmCounts += vmCountList.get(i);
            }
            meanAverageVmUsageRecords.add(sumOfAverages / averageVmUsageRecords.size());
            meanSumOfVmCounts.add(sumOfVmCounts / vmCountList.size());
            averageVmUsageRecords.clear();
            vmCountList.clear();
        }

        send(this.getId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE);
    }
}
