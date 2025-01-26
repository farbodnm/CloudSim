package org.cloudbus.cloudsim.serverless.components.scheduling;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterBroker;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.utils.CloudSimSCTags;
import org.cloudbus.cloudsim.serverlessbac.Constants;

import java.util.*;

@Slf4j
@Getter
@Setter
public class ServerlessController extends ContainerDatacenterBroker {

    protected ServerlessDatacenter datacenter;

    private RequestLoadBalancer loadBalancer;

    protected List<ServerlessContainer> containerDestroyedList;
    protected List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    protected List<Double> averageVmUsageRecords = new ArrayList<>();
    protected List<Integer> vmCountList = new ArrayList<>();
    protected List<Double> meanAverageVmUsageRecords = new ArrayList<>();
    protected List<Double> meanSumOfVmCounts = new ArrayList<>();

    public Queue<Double> requestArrivalTimes = new LinkedList<>();
    public Queue<ServerlessRequest> requestQueue = new LinkedList<>();

    protected double requestSubmitClock = 0;

    private int containersMadeCounter = 1;

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
        log.info("Request: {} arrived at: {}", request.getCloudletId(), CloudSim.clock());

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

    private void processRequestSubmitAck(SimEvent ev) {

    }

    private void processContainerDestroy(SimEvent ev) {

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
        log.info("Event sent to create to scale container: {} at time: {}", container.getId(), CloudSim.clock());
    }

    private void recordResourceUtilization(SimEvent ignored) {

    }

    /**
     * Load balancer functionalities
     */

    protected void sendFunctionRetryRequest(ServerlessRequest request) {
        send(
                getId(),
                Constants.FUNCTION_SCHEDULING_RETRY_DELAY,
                CloudSimSCTags.CLOUDLET_SUBMIT,
                request
        );
    }

    // TODO: why does every container requires a new request scheduler?
    protected void createContainer(ServerlessRequest request, String functionTypeId, int controllerId) {

        ServerlessContainer container =
                new ServerlessContainer(
                        containersMadeCounter++, // Used as container ID
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
}
