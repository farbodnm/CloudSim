package org.cloudbus.cloudsim.serverless.components.loadbalance;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.*;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.List;

@Slf4j
public class RequestLoadBalancerFirstFit implements RequestLoadBalancer {

  private final ServerlessController broker;

  public RequestLoadBalancerFirstFit(ServerlessController controller) {
    this.broker = controller;
  }

  @Override
  public void routeRequest(ServerlessRequest request) {
    if (request.retry > Constants.MAX_RESCHEDULE_TRIES) {
      broker.getCloudletList().remove(request);
      request.setSuccess(false);
      broker.getCloudletReceivedList().add(request);
    } else if (Constants.SCALE_PER_REQUEST) {
      broker.addToSubmitOnContainerCreation(request);
      broker.createContainer(request, request.getRequestFunctionId(), request.getUserId());
      broker.setRequestSubmitClock(CloudSim.clock());
    } else {
      boolean containerSelected = selectContainer(request);
      if (!containerSelected) {
        broker.getCloudletList().remove(request);
      }
    }
  }

  protected boolean selectContainer(ServerlessRequest task) {

    boolean contTypeExists = false;

    for (int x = 1; x <= broker.getVmsCreatedList().size(); x++) {
      ServerlessInvoker vm = (ServerlessInvoker) (ContainerVmList.getById(broker.getVmsCreatedList(), x));
      assert vm != null;
      if (vm.getFunctionContainerMap().containsKey(task.getRequestFunctionId())) {
        contTypeExists = true;
        List<Container> contList = vm.getFunctionContainerMap().get(task.getRequestFunctionId());
        for (Container container : contList) {
          ServerlessContainer cont = (ServerlessContainer) (container);
          ServerlessRequestScheduler clScheduler = (ServerlessRequestScheduler) (container.getContainerCloudletScheduler());
          if (clScheduler.isSuitableForRequest(task, cont)) {
            clScheduler.addToTotalCurrentAllocatedRamForRequests(task);
            clScheduler.addToTotalCurrentAllocatedMipsShareForRequests(task);
            log.info("{}: {}: Using idling container: container {}",
                CloudSim.clock(), this.getClass().getSimpleName(), cont.getId());

            task.setContainerId(cont.getId());
            broker.addToVmTaskMap(task, vm);
            cont.addToRunningTasks(task);
            cont.setIdling(false);
            cont.setIdleStartTime(0);

            broker.putInFunctionVmMap(vm, task.getRequestFunctionId());
            broker.setRequestSubmitClock(CloudSim.clock());
            broker.submitRequestToDC(task, vm.getId(), 0, cont.getId());
            return true;
          }
        }
      }
    }

    if (Constants.CONTAINER_CONCURRENCY && Constants.FUNCTION_HORIZONTAL_AUTOSCALING) {
      if (contTypeExists) {
        broker.sendFunctionRetryRequest(task);
        log.info("{}: {}: Container type exists so rescheduling",
            CloudSim.clock(), this.getClass().getSimpleName());
        task.retry++;
        return false;
      }
      for (int x = 1; x <= broker.getVmsCreatedList().size(); x++) {
        ServerlessInvoker vm = (ServerlessInvoker) (ContainerVmList.getById(broker.getVmsCreatedList(), x));
        assert vm != null;
        if (vm.getFunctionContainerMapPending().containsKey(task.getRequestFunctionId())) {
          log.info("{}: {}: Pending Container of type exists so rescheduling",
              CloudSim.clock(), this.getClass().getSimpleName());
          broker.sendFunctionRetryRequest(task);
          task.retry++;
          return false;
        }
      }

      log.info("{}: {}: Container type does not exist so creating new",
          CloudSim.clock(), this.getClass().getSimpleName());
      broker.createContainer(task, task.getRequestFunctionId(), task.getUserId());
      broker.sendFunctionRetryRequest(task);
      task.retry++;
      return false;
    } else {
      broker.addToSubmitOnContainerCreation(task);
      broker.createContainer(task, task.getRequestFunctionId(), task.getUserId());
      broker.setRequestSubmitClock(CloudSim.clock());
      return true;
    }
  }
}
