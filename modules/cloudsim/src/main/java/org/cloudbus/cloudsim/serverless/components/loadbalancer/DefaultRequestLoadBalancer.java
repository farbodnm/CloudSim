package org.cloudbus.cloudsim.serverless.components.loadbalancer;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessController;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.List;

/**
 * Loadbalancer class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

@Slf4j
public class DefaultRequestLoadBalancer extends RequestLoadBalancer{

    public DefaultRequestLoadBalancer(ServerlessController controller, ServerlessDatacenter datacenter) {
        super(controller, datacenter);
    }

    @Override
    public void routeRequest(ServerlessRequest request) {
        if (request.getRetry() > Constants.MAX_RESCHEDULE_TRIES) {
            controller.getCloudletList().remove(request);
            request.setSuccess(false);
            controller.getCloudletReceivedList().add(request);
        } else if (Constants.SCALE_PER_REQUEST) {
            controller.getRequestsToSubmitOnContainerCreation().add(request);
            controller.createContainer(request, request.getFunctionId(), request.getUserId());
            controller.setRequestSubmitClock(CloudSim.clock());
        } else {
            boolean containerSelected = selectContainer(request);
            if (!containerSelected) {
                controller.getCloudletList().remove(request);
                request.setSuccess(false);
            }
        }
    }

    /**
     * Local functionalities
     */

    protected boolean selectContainer(ServerlessRequest request) {

        boolean containerTypeExists = false;
        switch (Constants.CONTAINER_SELECTION_ALGO) {
            case FIRST_FIT: {
                for (int i = 1; i <= controller.getVmsCreatedList().size(); i++) {
                    ServerlessInvoker invoker = ContainerVmList.getById(controller.getVmsCreatedList(), i);
                    assert invoker != null;
                    if (invoker.getFunctionContainersMap().containsKey(request.getFunctionId())) {
                        containerTypeExists = true;
                    }

                    List<ServerlessContainer> containerList = invoker.getFunctionContainersMap().get(request.getFunctionId());
                    containerList.forEach(container -> {
                    });
                    for (ServerlessContainer container : containerList) {
                        ServerlessRequestScheduler requestScheduler = (ServerlessRequestScheduler) container.getContainerCloudletScheduler();
                        if (requestScheduler.isSuitableForRequest(request, container)) {
                            requestScheduler.addToTotalCurrentAllocatedRamForRequests(request);
                            requestScheduler.addToTotalCurrentAllocatedCpuForRequests(request);
                            log.info("{}: {}: Using idling container in invoker: {}",
                                    CloudSim.clock(), "LoadBalancer#1", invoker.getId());
                            request.setContainerId(container.getId());
                            controller.addToInvokerRequestMap(request, invoker);
                            container.addToRunningRequests(request);
                            container.setIdling(false);
                            container.setIdleStartTime(0);

                            controller.addToFunctionInvokersMap(invoker, request.getFunctionId());
                            controller.setRequestSubmitClock(CloudSim.clock());
                            controller.submitRequestToDC(request, invoker.getId(), 0, container.getId());
                            return true;
                        }
                    }
                }
                break;
            }
        }

        if (Constants.CONTAINER_CONCURRENCY && Constants.FUNCTION_HORIZONTAL_AUTOSCALING) {
            if (containerTypeExists) {
                log.info("{}: {}: No suitable container found but container type already exists so rescheduling request: {}",
                        CloudSim.clock(), "LoadBalancer#1", request.getCloudletId());
                controller.sendFunctionRetryRequest(request);
                request.incrementRetryCount();
                return false;
            }

            for (int i = 1; i <= controller.getVmsCreatedList().size(); i++) {
                ServerlessInvoker invoker = ContainerVmList.getById(controller.getVmsCreatedList(), i);
                assert invoker != null;
                if (invoker.getPendingFunctionContainerMap().containsKey(request.getFunctionId())) {
                    log.info("{}: {}: No suitable container found but container type is already in pending list so rescheduling request: {}",
                            CloudSim.clock(), "LoadBalancer#1", request.getCloudletId());
                    controller.sendFunctionRetryRequest(request);
                    request.incrementRetryCount();
                    return false;
                }
            }

            log.info("{}: {}: Container type didn't exist so creating container for request: {}",
                    CloudSim.clock(), "LoadBalancer#1", request.getCloudletId());
            controller.createContainer(request, request.getFunctionId(), request.getUserId());
            controller.sendFunctionRetryRequest(request);
            request.incrementRetryCount();
            return false;
        } else {
            controller.getRequestsToSubmitOnContainerCreation().add(request);
            controller.createContainer(request, request.getFunctionId(), request.getUserId());
            controller.setRequestSubmitClock(CloudSim.clock());
            return true;
        }
    }
}
