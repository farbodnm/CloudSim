package org.cloudbus.cloudsim.serverless.components.schedule;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.List;
import java.util.Objects;
@Slf4j
public class FunctionSchedulerConstBased extends FunctionScheduler {

  // Vm index for selecting Vm in round-robin fashion
  private int selectedVmIndex = 1;

  @Override
  public boolean allocateVmForContainer(Container container, ContainerVm containerVm, List<ContainerVm> containerVmList) {
    setContainerVmList(containerVmList);

    if (containerVm == null) {
      log.error("{}: {}: No suitable invoker found for container: {}",
          CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
      return false;
    }
    if (containerVm.containerCreate(container)) { // if vm has been successfully created in the host
      getContainerTable().put(container.getUid(), containerVm);
      log.info("{}: {}: Container: {} has been allocated to invoker: :{}",
          CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), containerVm.getId());
      return true;
    }
    log.error("{}: {}: Creation of container: {} on the invoker: {} failed unexpectedly",
        CloudSim.clock(), this.getClass().getSimpleName(), container.getId(), containerVm.getId());
    return false;
  }

  @Override
  public ContainerVm findVmForContainer(Container container) {

    ServerlessInvoker selectedVm = null;
    boolean vmSelected = false;

    try {
      switch (Constants.INVOKER_SELECTION_ALGO) {

        case ROUND_ROBIN: {
          for (int x = selectedVmIndex; x <= getContainerVmList().size(); x++) {
            ServerlessInvoker tempSelectedVm = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), x));
            if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
              selectedVm = tempSelectedVm;
              vmSelected = true;
              break;
            }
          }

          if (!vmSelected) {
            for (int x = 1; x < selectedVmIndex; x++) {
              ServerlessInvoker tempSelectedVm = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), x));
              if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                selectedVm = tempSelectedVm;
                break;
              }
            }
          }

          if (selectedVmIndex == getContainerVmList().size()) {
            selectedVmIndex = 1;
          } else
            selectedVmIndex++;
          break;
        }

        case BEST_FIT_FIRST: {
          for (int x = 1; x <= getContainerVmList().size(); x++) {
            ServerlessInvoker tempSelectedVm = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), x));
            if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
              selectedVm = tempSelectedVm;
              break;
            }
          }
          break;
        }

        case BEST_FIT_BOUNDED: {
          double minRemainingCap = Double.MAX_VALUE;
          for (int x = 1; x <= getContainerVmList().size(); x++) {
            ServerlessInvoker tempSelectedVm = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), x));
            double vmCpuAvailability = tempSelectedVm.getAvailableMips() / tempSelectedVm.getTotalMips();
            if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
              if (vmCpuAvailability < minRemainingCap) {
                selectedVm = tempSelectedVm;
                minRemainingCap = vmCpuAvailability;
              }
            }
          }
        }

        log.info(
            "{}: {}: Selected vm: {} for container: {} using {} algorithm",
            CloudSim.clock(),
            this.getClass().getSimpleName(),
            selectedVmIndex,
            container.getId(),
            Constants.INVOKER_SELECTION_ALGO.name()
        );
      }
    } catch (Exception ex) {
      if (selectedVm != null) {
        log.info(
            "{}: {}: Selected vm: {} for container: {} using {} algorithm but an unexpected exception occurred: {}",
            CloudSim.clock(),
            this.getClass().getSimpleName(),
            selectedVmIndex,
            container.getId(),
            Constants.INVOKER_SELECTION_ALGO.name(),
            ex.getMessage()
        );
      } else {
        log.error("{}: {}: Failed to select invoker for the container: {}, it will be stranded",
            CloudSim.clock(), this.getClass().getSimpleName(), container.getId());
      }
    }

    return selectedVm;
  }
}
