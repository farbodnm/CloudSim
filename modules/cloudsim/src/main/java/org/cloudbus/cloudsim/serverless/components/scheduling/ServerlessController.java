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

    }

    private void processRequestSubmitAck(SimEvent ev) {

    }

    private void processContainerDestroy(SimEvent ev) {

    }

    private void recordResourceUtilization(SimEvent ignored) {

    }
}
