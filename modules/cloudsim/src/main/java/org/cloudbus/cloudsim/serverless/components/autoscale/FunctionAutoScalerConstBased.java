package org.cloudbus.cloudsim.serverless.components.autoscale;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.*;

/**
 * Doesn't seem to function correctly
 */

@Slf4j
public class FunctionAutoScalerConstBased implements FunctionAutoScaler {

  @Setter
  protected ServerlessDatacenter datacenter;

  protected int userId ;
  protected final String CONTAINER_COUNT = "containerCount";

  protected final String CONTAINER_PENDING_COUNT = "pendingContainerCount";
  protected final String CONTAINER_CPU_UTIL = "cpuUtilization";
  protected final String CONTAINER_MIPS = "containerMips";
  protected final String CONTAINER_RAM = "containerRam";
  protected final String CONTAINER_PES = "containerPes";
  protected final String CPU_ACTIONS = "cpuActions";
  protected final String MEMORY_ACTIONS = "memActions";

  public FunctionAutoScalerConstBased(ServerlessDatacenter datacenter) {
    this.datacenter = datacenter;
  }

  @Override
  public void scaleFunctions() {

    Map.Entry<Map<String, Map<String, Double>>, Map<String, List<ServerlessContainer>>> funcData = containerScalingTrigger();

    if (Constants.FUNCTION_HORIZONTAL_AUTOSCALING) {
      containerHorizontalAutoScaler(funcData.getKey(), funcData.getValue());
    }

    if (Constants.FUNCTION_VERTICAL_AUTOSCALING) {
      List<? extends ContainerHost> list = datacenter.getVmAllocationPolicy().getContainerHostList();
      Map<String, Map<String, List<Integer>>> unAvailableActionMap = containerVerticalAutoScaler();
      int cpuIncrement = 0;
      int memIncrement = 0;
      double cpuUtilization = 0;
      String scalingFunction = null;

      for (Map.Entry<String, Map<String, Double>> data : funcData.getKey().entrySet()) {
        if (data.getValue().get(CONTAINER_COUNT) > 0) {
          if (
              data.getValue().get(CONTAINER_CPU_UTIL) / data.getValue().get(CONTAINER_COUNT)
                  > Constants.CONTAINER_SCALE_CPU_THRESHOLD
                  && data.getValue().get(CONTAINER_CPU_UTIL) / data.getValue().get(CONTAINER_COUNT)
                  > cpuUtilization
          ) {
            cpuUtilization = data.getValue().get(CONTAINER_CPU_UTIL) / data.getValue().get(CONTAINER_COUNT);
            scalingFunction = data.getKey();
          }
        }
      }

      if (scalingFunction != null) {
        log.info("{}: {}: Scaling function: {}",
            CloudSim.clock(), this.getClass().getSimpleName(), scalingFunction);

        for (int x = 0; x < Constants.CONTAINER_MIPS_INCREMENT.length; x++) {
          if (!unAvailableActionMap.get(CPU_ACTIONS).get(scalingFunction).contains(Constants.CONTAINER_MIPS_INCREMENT[x])) {
            cpuIncrement = Constants.CONTAINER_MIPS_INCREMENT[x];
            break;
          }
        }

        for (int x = 0; x < Constants.CONTAINER_RAM_INCREMENT.length; x++) {
          if (!unAvailableActionMap.get(MEMORY_ACTIONS).get(scalingFunction).contains(Constants.CONTAINER_RAM_INCREMENT[x])) {
            memIncrement = Constants.CONTAINER_RAM_INCREMENT[x];
            break;
          }
        }

        for (ContainerHost host : list) {
          for (ContainerVm machine : host.getVmList()) {
            userId = machine.getUserId();
            ServerlessInvoker vm = (ServerlessInvoker) machine;
            if (vm.getFunctionContainerMap().containsKey(scalingFunction)) {
              for (Container cont : vm.getFunctionContainerMap().get(scalingFunction)) {
                log.info("{}: {}: Scaled container: {} running on vm: {} incrementation: mem: {}  cpu: {}",
                    CloudSim.clock(),
                    this.getClass().getSimpleName(),
                    cont.getId(),
                    cont.getVm().getId(),
                    memIncrement,
                    cpuIncrement
                );
                datacenter.containerVerticalScale(cont, vm, cpuIncrement, memIncrement);
              }
            }
          }
        }
      }
    }
  }

  protected Map.Entry<Map<String, Map<String, Double>>, Map<String, List<ServerlessContainer>>> containerScalingTrigger() {

    Map<String, Map<String, Double>> fnNestedMap = new HashMap<>();
    Map<String, List<ServerlessContainer>> emptyContainers = new HashMap<>();
    switch (Constants.SCALING_TRIGGER_LOGIC) {
      /** Triggering scaling based on cpu threshold method **/
      case CPU_THRESHOLD: {
        List<? extends ContainerHost> list = datacenter.getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
          for (ContainerVm machine : host.getVmList()) {
            userId = machine.getUserId();
            ServerlessInvoker vm = (ServerlessInvoker) machine;
            for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMap().entrySet()) {
              if (fnNestedMap.containsKey(contMap.getKey())) {

                log.info("{}: {}: adding new key: {} to getFunctionMap",
                    CloudSim.clock(), this.getClass().getSimpleName(), contMap.getKey());

                fnNestedMap.get(
                    contMap.getKey()).put(CONTAINER_COUNT,
                    fnNestedMap.get(contMap.getKey()).get(CONTAINER_COUNT) + contMap.getValue().size()
                );
                if (!contMap.getValue().isEmpty()) {
                  fnNestedMap.get(contMap.getKey()).put(
                      CONTAINER_MIPS,
                      (contMap.getValue()).get(0).getMips()
                  );
                  fnNestedMap.get(contMap.getKey()).put(
                      CONTAINER_RAM,
                      Double.parseDouble(Float.toString(((contMap.getValue()).get(0).getRam())))
                  );
                  fnNestedMap.get(contMap.getKey()).put(
                      CONTAINER_PES,
                      Double.parseDouble(Integer.toString(((contMap.getValue()).get(0).getNumberOfPes())))
                  );
                }
              } else {

                log.info("{}: {}: getFunctionContainerMap contains key: {} so not adding...",
                    CloudSim.clock(), this.getClass().getSimpleName(), contMap.getKey());

                Map<String, Double> fnMap = new HashMap<>();
                fnMap.put(CONTAINER_COUNT, (double) contMap.getValue().size());
                fnMap.put(CONTAINER_PENDING_COUNT, 0.0);
                fnMap.put(CONTAINER_CPU_UTIL, 0.0);
                if (!contMap.getValue().isEmpty()) {
                  fnMap.put(
                      CONTAINER_MIPS,
                      (contMap.getValue()).get(0).getMips()
                  );
                  fnMap.put(
                      CONTAINER_RAM,
                      Double.parseDouble(Float.toString(((contMap.getValue()).get(0).getRam())))
                  );
                  fnMap.put(
                      CONTAINER_PES,
                      Double.parseDouble(Integer.toString(((contMap.getValue()).get(0).getNumberOfPes())))
                  );
                } else {
                  fnMap.put(CONTAINER_MIPS, 0.0);
                  fnMap.put(CONTAINER_RAM, 0.0);
                  fnMap.put(CONTAINER_PES, 0.0);
                }

                fnNestedMap.put(contMap.getKey(), fnMap);
              }
              for (Container cont : contMap.getValue()) {
                ServerlessContainer container = (ServerlessContainer) cont;
                if (container.getRunningTasks().isEmpty()) {
                  if (!emptyContainers.containsKey(contMap.getKey())) {
                    emptyContainers.put(contMap.getKey(), new ArrayList<>());
                  }
                  emptyContainers.get(contMap.getKey()).add(container);
                }
                fnNestedMap.get(contMap.getKey()).put(CONTAINER_CPU_UTIL,
                    Double.sum(fnNestedMap.get(contMap.getKey()).get(CONTAINER_CPU_UTIL),
                        (((ServerlessRequestScheduler) (container.getContainerCloudletScheduler()))
                            .getTotalCurrentAllocatedMipsShareForRequests())));
              }
            }
            for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMapPending().entrySet()) {
              if (fnNestedMap.containsKey(contMap.getKey())) {

                log.info("{}: {}: fnNestedMap contains key: {} so not adding...",
                    CloudSim.clock(), this.getClass().getSimpleName(), contMap.getKey());

                fnNestedMap.get(contMap.getKey())
                    .put(CONTAINER_PENDING_COUNT,
                        fnNestedMap.get(contMap.getKey()).get(CONTAINER_PENDING_COUNT)
                            + contMap.getValue().size());

                if (!contMap.getValue().isEmpty()) {
                  fnNestedMap.get(contMap.getKey()).put(
                      CONTAINER_MIPS,
                      (contMap.getValue()).get(0).getMips()
                  );
                  fnNestedMap.get(contMap.getKey()).put(
                      CONTAINER_RAM,
                      Double.parseDouble(Float.toString(((contMap.getValue()).get(0).getRam())))
                  );
                  fnNestedMap.get(contMap.getKey()).put(
                      CONTAINER_PES,
                      Double.parseDouble(Integer.toString(((contMap.getValue()).get(0).getNumberOfPes())))
                  );
                }
              } else {

                log.info("{}: {}: adding key: {} to fnNestedMap",
                    CloudSim.clock(), this.getClass().getSimpleName(), contMap.getKey());

                Map<String, Double> fnMap = new HashMap<>();
                fnMap.put(CONTAINER_PENDING_COUNT, (double) contMap.getValue().size());
                fnMap.put(CONTAINER_COUNT, 0.0);
                fnMap.put(CONTAINER_CPU_UTIL, 0.0);
                if (!contMap.getValue().isEmpty()) {
                  fnMap.put(
                      CONTAINER_MIPS,
                      (((contMap.getValue()).get(0))).getMips()
                  );
                  fnMap.put(
                      CONTAINER_RAM,
                      Double.parseDouble(Float.toString(((contMap.getValue()).get(0).getRam())))
                  );
                  fnMap.put(
                      CONTAINER_PES,
                      Double.parseDouble(Float.toString(((contMap.getValue()).get(0).getNumberOfPes())))
                  );
                } else {
                  fnMap.put(CONTAINER_MIPS, 0.0);
                  fnMap.put(CONTAINER_RAM, 0.0);
                  fnMap.put(CONTAINER_PES, 0.0);
                }

                fnNestedMap.put(contMap.getKey(), fnMap);
              }
            }

          }
        }
        break;
      }
    }
    return new AbstractMap.SimpleEntry<>(fnNestedMap, emptyContainers);
  }

  protected void containerHorizontalAutoScaler(Map<String, Map<String, Double>> fnNestedMap, Map<String, List<ServerlessContainer>> emptyContainers) {
    switch (Constants.HOR_SCALING_LOGIC) {
      case CPU_THRESHOLD: {
        for (Map.Entry<String, Map<String, Double>> data : fnNestedMap.entrySet()) {
          int desiredReplicas = 0;
          if (data.getValue().get(CONTAINER_COUNT) > 0) {
            desiredReplicas = (int) Math.ceil(
                data.getValue().get(CONTAINER_COUNT)
                    * (data.getValue().get(CONTAINER_CPU_UTIL)
                    / data.getValue().get(CONTAINER_COUNT)
                    / Constants.CONTAINER_SCALE_CPU_THRESHOLD)
            );
          }
          int newReplicaCount;
          int newReplicasToCreate;
          int replicasToRemove;
          newReplicaCount = Math.min(desiredReplicas, Constants.MAX_REPLICAS);

          log.info("{}: {}: function: {} needed replica count of: {} while existing count is: {}",
              CloudSim.clock(),
              this.getClass().getSimpleName(),
              data.getKey(),
              newReplicaCount,
              data.getValue().get(CONTAINER_COUNT) + data.getValue().get(CONTAINER_PENDING_COUNT)
          );

          if (newReplicaCount > (data.getValue().get(CONTAINER_COUNT) + data.getValue().get(CONTAINER_PENDING_COUNT))) {
            newReplicasToCreate = (int) Math.ceil(
                newReplicaCount
                    - data.getValue().get(CONTAINER_COUNT)
                    - data.getValue().get(CONTAINER_PENDING_COUNT)
            );

            for (int x = 0; x < newReplicasToCreate; x++) {
              String[] dt = new String[5];
              dt[0] = Integer.toString(userId);
              dt[1] = data.getKey();
              dt[2] = Double.toString(data.getValue().get(CONTAINER_MIPS));
              dt[3] = Double.toString(data.getValue().get(CONTAINER_RAM));
              dt[4] = Double.toString(data.getValue().get(CONTAINER_PES));

              datacenter.sendScaledContainerCreationRequest(dt);
            }
          }

          if (newReplicaCount < (data.getValue().get(CONTAINER_COUNT) + data.getValue().get(CONTAINER_PENDING_COUNT))) {
            replicasToRemove = (int) Math.ceil(
                data.getValue().get(CONTAINER_COUNT)
                    + data.getValue().get(CONTAINER_PENDING_COUNT)
                    - newReplicaCount
            );
            int removedContainers = 0;
            if (emptyContainers.containsKey(data.getKey())) {
              for (ServerlessContainer cont : emptyContainers.get(data.getKey())) {
                datacenter.getContainersToDestroy().add(cont);
                removedContainers++;
                if (removedContainers == replicasToRemove) {
                  break;
                }
              }
            }
          }
        }
        break;
      }
    }
  }


  protected Map<String, Map<String, List<Integer>>> containerVerticalAutoScaler() {

    Map<String, Map<String, List<Integer>>> unAvailableActionMap = new HashMap<>();
    double peMIPSForContainerType = 0;
    double ramForContainerType = 0;
    double pesForContainerType = 0;
    Map<String, List<Integer>> unAvailableActionListCPU = new HashMap<>();
    Map<String, List<Integer>> unAvailableActionListRam = new HashMap<>();
    List<? extends ContainerHost> list = datacenter.getVmAllocationPolicy().getContainerHostList();
    for (ContainerHost host : list) {
      for (ContainerVm machine : host.getVmList()) {
        ServerlessInvoker vm = (ServerlessInvoker) machine;
        double vmUsedUpRam =
            vm.getContainerRamProvisioner().getRam() - vm.getContainerRamProvisioner().getAvailableVmRam();
        double vmUsedUpMIPS =
            vm.getContainerScheduler().getPeCapacity()
                * vm.getContainerScheduler().getPeList().size()
                - vm.getContainerScheduler().getAvailableMips();

        for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMap().entrySet()) {
          double containerCPUUtilMin = 0;
          double containerRAMUtilMin = 0;
          int numContainers = contMap.getValue().size();
          String functionId = contMap.getKey();
          for (Container cont : contMap.getValue()) {
            peMIPSForContainerType = cont.getMips();
            ramForContainerType = cont.getRam();
            pesForContainerType = cont.getNumberOfPes();
            ServerlessContainer container = (ServerlessContainer) cont;
            ServerlessRequestScheduler clScheduler = (ServerlessRequestScheduler) (container.getContainerCloudletScheduler());
            if (clScheduler.getTotalCurrentAllocatedMipsShareForRequests() > containerCPUUtilMin) {
              containerCPUUtilMin = clScheduler.getTotalCurrentAllocatedMipsShareForRequests();
            }
            if (clScheduler.getTotalCurrentAllocatedRamForRequests() > containerRAMUtilMin) {
              containerRAMUtilMin = clScheduler.getTotalCurrentAllocatedRamForRequests();
            }
          }
          for (int x = 0; x < (Constants.CONTAINER_RAM_INCREMENT).length; x++) {
            if (unAvailableActionListRam.containsKey(functionId)) {
              if (!unAvailableActionListRam.get(functionId).contains(x)) {
                if (
                    Constants.CONTAINER_RAM_INCREMENT[x] * numContainers > vm.getContainerRamProvisioner().getAvailableVmRam()
                        || Constants.CONTAINER_RAM_INCREMENT[x] * numContainers + vmUsedUpRam < 0
                        || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) > Constants.MAX_CONTAINER_RAM
                        || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) < Constants.MIN_CONTAINER_RAM
                        || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) < containerRAMUtilMin * ramForContainerType
                ) {
                  unAvailableActionListRam.get(functionId).add(Constants.CONTAINER_RAM_INCREMENT[x]);
                }
              }
            } else {
              List<Integer> listRam = new ArrayList<>();
              unAvailableActionListRam.put(functionId, listRam);
            }
          }
          for (int x = 0; x < (Constants.CONTAINER_MIPS_INCREMENT).length; x++) {
            if (unAvailableActionListCPU.containsKey(functionId)) {
              if (!unAvailableActionListCPU.get(functionId).contains(x)) {
                if (
                    Constants.CONTAINER_MIPS_INCREMENT[x] * numContainers * pesForContainerType > vm.getContainerScheduler().getAvailableMips()
                        || Constants.CONTAINER_MIPS_INCREMENT[x] * numContainers * pesForContainerType + vmUsedUpMIPS < 0
                        || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) > Constants.MAX_CONTAINER_MIPS
                        || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) < Constants.MIN_CONTAINER_MIPS
                        || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) < containerCPUUtilMin * peMIPSForContainerType
                ) {
                  unAvailableActionListCPU.get(functionId).add(Constants.CONTAINER_MIPS_INCREMENT[x]);
                }
              }
            } else {
              ArrayList<Integer> listCpu = new ArrayList<>();
              unAvailableActionListCPU.put(functionId, listCpu);
            }
          }

        }


      }
    }

    log.info("{}: {}: Cpu unavailable: {}",
        CloudSim.clock(), this.getClass().getSimpleName(), unAvailableActionListCPU);
    log.info("{}: {}: Memory unavailable: {}",
        CloudSim.clock(), this.getClass().getSimpleName(), unAvailableActionListRam);

    unAvailableActionMap.put(MEMORY_ACTIONS, unAvailableActionListCPU);
    unAvailableActionMap.put(CPU_ACTIONS, unAvailableActionListRam);
    return unAvailableActionMap;
  }
}
