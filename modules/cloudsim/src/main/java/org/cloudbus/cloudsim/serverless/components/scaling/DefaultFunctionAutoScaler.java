package org.cloudbus.cloudsim.serverless.components.scaling;

import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.*;

/**
 * The default container scaler that is based on constants
 *
 * @see Constants
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
public class DefaultFunctionAutoScaler extends FunctionAutoScaler {

    public DefaultFunctionAutoScaler(ServerlessDatacenter datacenter) {
        super(datacenter);
    }

    @Override
    public void scaleFunctions() {

        Map.Entry<Map<String, Map<String, Double>>, Map<String, List<ServerlessContainer>>> functionData = containerScalingTrigger();
        if (Constants.FUNCTION_HORIZONTAL_AUTOSCALING) {
            containerHorizontalScaler(functionData.getKey(), functionData.getValue());
        }
        if (Constants.FUNCTION_VERTICAL_AUTOSCALING) {
            List<? extends ContainerHost> hostList = getDatacenter().getVmAllocationPolicy().getContainerHostList();
            Map<String, Map<String, List<Integer>>> unAvailableActionMap = containerVerticalAutoScaler();
            int cpuIncrement = 0;
            int memIncrement = 0;
            double cpuUtilization = 0D;
            String scalingFunction = null;

            for (Map.Entry<String, Map<String, Double>> data : functionData.getKey().entrySet()) {
                if (data.getValue().get(CONTAINER_COUNT) > 0) {
                    if (data.getValue().get(CONTAINER_CPU_UTIL) / data.getValue().get(CONTAINER_COUNT) > Constants.CONTAINER_SCALE_CPU_THRESHOLD
                            && data.getValue().get(CONTAINER_CPU_UTIL) / data.getValue().get(CONTAINER_COUNT) > cpuUtilization
                    ) {
                        cpuUtilization = data.getValue().get(CONTAINER_CPU_UTIL) / data.getValue().get(CONTAINER_COUNT);
                        scalingFunction = data.getKey();
                    }
                }
            }

            if (scalingFunction != null) {

                for (int i = 0; i < Constants.CONTAINER_MIPS_INCREMENT.length; i++) {
                    if (!unAvailableActionMap.get(CPU_ACTIONS).get(scalingFunction).contains(Constants.CONTAINER_MIPS_INCREMENT[i])) {
                        cpuIncrement = Constants.CONTAINER_MIPS_INCREMENT[i];
                        break;
                    }
                }

                for (int i = 0; i < Constants.CONTAINER_RAM_INCREMENT.length; i++) {
                    if (!unAvailableActionMap.get(MEMORY_ACTIONS).get(scalingFunction).contains(Constants.CONTAINER_RAM_INCREMENT[i])) {
                        memIncrement = Constants.CONTAINER_RAM_INCREMENT[i];
                        break;
                    }
                }

                for (ContainerHost host: hostList) {
                    for (ContainerVm machine: host.getVmList()) {
                        setUserId(machine.getUserId());
                        ServerlessInvoker invoker = (ServerlessInvoker) machine;
                        if (invoker.getFunctionContainersMap().containsKey(scalingFunction)) {
                            for (ServerlessContainer container: invoker.getFunctionContainersMap().get(scalingFunction)) {
                                getDatacenter().containerVerticalScale(container, invoker, cpuIncrement, memIncrement);
                            }
                        }
                    }
                }
            }
        }
    }

    protected Map.Entry<Map<String, Map<String, Double>>, Map<String, List<ServerlessContainer>>> containerScalingTrigger() {

        Map<String, Map<String, Double>> functionNestedMap = new HashMap<>();
        Map<String, List<ServerlessContainer>> emptyContainers = new HashMap<>();

        switch (Constants.SCALING_TRIGGER_LOGIC) {
            case CPU_THRESHOLD:
                List<? extends ContainerHost> hostList = getDatacenter().getVmAllocationPolicy().getContainerHostList();
                for (ContainerHost host : hostList) {
                    for (ContainerVm machine : host.getVmList()) {
                        ServerlessInvoker invoker = (ServerlessInvoker) machine;
                        setUserId(invoker.getUserId());
                        invoker.getFunctionContainersMap().forEach((key, value) -> {
                            if (functionNestedMap.containsKey(key)) {
                                addToFunctionMap(functionNestedMap, key, value, CONTAINER_COUNT);
                            } else {
                                Map<String, Double> functionMap = createFunctionMap(value);
                                functionNestedMap.put(key, functionMap);
                            }
                            value.forEach((container -> {
                                if (container.getRunningRequests().isEmpty()) {
                                    emptyContainers.computeIfAbsent(key, k -> new ArrayList<>()).add(container);
                                }
                                functionNestedMap.get(key).put(
                                        CONTAINER_CPU_UTIL,
                                        Double.sum(
                                                functionNestedMap.get(key).get(CONTAINER_CPU_UTIL),
                                                ((ServerlessRequestScheduler) container.getContainerCloudletScheduler())
                                                        .getTotalCurrentAllocatedMipsShareForRequests()
                                        )
                                );
                            }));
                        });
                        invoker.getPendingFunctionContainerMap().forEach((key, value) -> {
                            if (functionNestedMap.containsKey(key)) {
                                addToFunctionMap(functionNestedMap, key, value, CONTAINER_PENDING_COUNT);
                            } else {
                                Map<String, Double> functionMap = createFunctionMap(value);
                                functionNestedMap.put(key, functionMap);
                            }
                        });
                    }
                }
        }

        return new AbstractMap.SimpleEntry<>(functionNestedMap, emptyContainers);
    }

    private Map<String, Double> createFunctionMap(List<ServerlessContainer> value) {
        Map<String, Double> functionMap = new HashMap<>();
        functionMap.put(CONTAINER_COUNT, (double) value.size());
        functionMap.put(CONTAINER_PENDING_COUNT, 0D);
        functionMap.put(CONTAINER_CPU_UTIL, 0D);
        if (!value.isEmpty()) {
            functionMap.put(CONTAINER_MIPS, value.get(0).getMips());
            functionMap.put(CONTAINER_RAM, (double) value.get(0).getRam());
            functionMap.put(CONTAINER_PES, (double) value.get(0).getNumberOfPes());
        } else {
            functionMap.put(CONTAINER_MIPS, 0D);
            functionMap.put(CONTAINER_RAM, 0D);
            functionMap.put(CONTAINER_PES, 0D);
        }
        return functionMap;
    }

    private void addToFunctionMap(Map<String, Map<String, Double>> functionNestedMap, String key, List<ServerlessContainer> value, String containerPendingCount) {
        functionNestedMap.get(key).put(
                containerPendingCount,
                functionNestedMap.get(key).get(containerPendingCount)
                        + value.size()
        );
        if (!value.isEmpty()) {
            Map<String, Double> functionMap = functionNestedMap.get(key);
            functionMap.put(CONTAINER_MIPS, value.get(0).getMips());
            functionMap.put(CONTAINER_RAM, (double) value.get(0).getRam());
            functionMap.put(CONTAINER_PES, (double) value.get(0).getNumberOfPes());
        }
    }

    protected void containerHorizontalScaler(Map<String, Map<String, Double>> functionNestedMap, Map<String, List<ServerlessContainer>> emptyContainers) {

        switch (Constants.HOR_SCALING_LOGIC) {
            case CPU_THRESHOLD:
                functionNestedMap.forEach((key, value) -> {

                    int desiredReplicas = 0;

                    // Hmm
                    if (value.get(CONTAINER_COUNT) > 0) {
                        desiredReplicas = (int) Math.ceil(value.get(CONTAINER_COUNT)
                                * (value.get(CONTAINER_CPU_UTIL)
                                / value.get(CONTAINER_COUNT)
                                / Constants.CONTAINER_SCALE_CPU_THRESHOLD));
                    }
                    int newReplicasCount;
                    int newReplicasToCreate;
                    int replicasToRemove;
                    newReplicasCount = Math.min(desiredReplicas, Constants.MAX_REPLICAS);

                    if (newReplicasCount > (value.get(CONTAINER_COUNT) + value.get(CONTAINER_PENDING_COUNT))) {
                        newReplicasToCreate = (int) Math.ceil(newReplicasCount - value.get(CONTAINER_COUNT) - value.get(CONTAINER_PENDING_COUNT));
                        for (int i = 0; i < newReplicasToCreate; i++) {
                            String[] dt = new String[]{
                                    Integer.toString(getUserId()),
                                    key,
                                    Double.toString(value.get(CONTAINER_MIPS)),
                                    Double.toString(value.get(CONTAINER_RAM)),
                                    Double.toString(value.get(CONTAINER_PES))
                            };
                            getDatacenter().sendScaledContainerCreationRequest(dt);
                        }
                    } else if (newReplicasCount < value.get(CONTAINER_COUNT) + value.get(CONTAINER_PENDING_COUNT)) {
                        replicasToRemove =
                                (int) Math.ceil(value.get(CONTAINER_COUNT) + value.get(CONTAINER_PENDING_COUNT) - newReplicasCount);
                        int removedContainersCount = 0;
                        if (emptyContainers.containsKey(key)) {
                            for (ServerlessContainer container: emptyContainers.get(key)) {
                                getDatacenter().getContainersToDestroy().add(container);
                                removedContainersCount++;
                                if (removedContainersCount == replicasToRemove) {
                                    break;
                                }
                            }
                        }
                    }
                });
        }
    }

    protected Map<String, Map<String, List<Integer>>> containerVerticalAutoScaler() {

        Map<String, Map<String, List<Integer>>> unavailableActionMap = new HashMap<>();
        double peMIPSForContainerType = 0;
        double ramForContainerType = 0;
        double pesForContainerType = 0;
        Map<String, List<Integer>> unavailableActionListCPU = new HashMap<>();
        Map<String, List<Integer>> unavailableActionListRAM = new HashMap<>();
        List<? extends ContainerHost> list = getDatacenter().getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
            for (ContainerVm machine: host.getVmList()) {
                ServerlessInvoker invoker = (ServerlessInvoker) machine;
                double invokerUsedUpRAM = invoker.getContainerRamProvisioner().getRam() - invoker.getContainerRamProvisioner().getAvailableVmRam();
                double invokerUsedUpMIPS = invoker.getContainerScheduler().getPeCapacity()
                        * invoker.getContainerScheduler().getPeList().size() - invoker.getContainerScheduler().getAvailableMips();
                for (Map.Entry<String, List<ServerlessContainer>> containerMap: invoker.getFunctionContainersMap().entrySet()) {
                    double containerCPUUtilMin = 0D;
                    double containerRAMUtilMin = 0D;

                    int numContainers = containerMap.getValue().size();
                    String functionId = containerMap.getKey();

                    for (ServerlessContainer container: containerMap.getValue()) {
                        peMIPSForContainerType = container.getMips();
                        ramForContainerType = container.getRam();
                        pesForContainerType = container.getNumberOfPes();
                        ServerlessRequestScheduler scheduler = (ServerlessRequestScheduler) container.getContainerCloudletScheduler();
                        if (scheduler.getTotalCurrentAllocatedMipsShareForRequests() > containerCPUUtilMin) {
                            containerCPUUtilMin = scheduler.getTotalCurrentAllocatedMipsShareForRequests();
                        }
                        if (scheduler.getTotalCurrentAllocatedRamForRequests() > containerRAMUtilMin) {
                            containerRAMUtilMin = scheduler.getTotalCurrentAllocatedRamForRequests();
                        }
                    }

                    for (int i = 0; i < (Constants.CONTAINER_RAM_INCREMENT).length; i++) {
                        if (unavailableActionListRAM.containsKey(functionId)) {
                            if (unavailableActionListRAM.get(functionId).contains(i)) {
                                if (Constants.CONTAINER_RAM_INCREMENT[i] * numContainers > invoker.getContainerRamProvisioner().getAvailableVmRam()
                                        || Constants.CONTAINER_RAM_INCREMENT[i] * numContainers + invokerUsedUpRAM < 0
                                        || ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[i] > Constants.MAX_CONTAINER_RAM
                                        || ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[i] < Constants.MIN_CONTAINER_RAM
                                        || ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[i] < containerRAMUtilMin * ramForContainerType
                                ) {
                                    unavailableActionListRAM.get(functionId).add(Constants.CONTAINER_RAM_INCREMENT[i]);
                                }
                            }
                        } else {
                            List<Integer> listRam = new ArrayList<>();
                            unavailableActionListRAM.put(functionId, listRam);
                        }
                    }

                    for (int i = 0; i < Constants.CONTAINER_MIPS_INCREMENT.length; i++) {
                        if (unavailableActionListCPU.containsKey(functionId)) {
                            if (!unavailableActionListCPU.get(functionId).contains(i)) {
                                if (Constants.CONTAINER_MIPS_INCREMENT[i] * numContainers * pesForContainerType > invoker.getContainerScheduler().getAvailableMips()
                                        || Constants.CONTAINER_MIPS_INCREMENT[i] * numContainers * pesForContainerType + invokerUsedUpMIPS < 0
                                        || peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[i] > Constants.MAX_CONTAINER_MIPS
                                        || peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[i] < Constants.MIN_CONTAINER_MIPS
                                        || peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[i] < containerCPUUtilMin + peMIPSForContainerType
                                ) {
                                    unavailableActionListCPU.get(functionId).add(Constants.CONTAINER_MIPS_INCREMENT[i]);
                                }
                            }
                        } else {
                            List<Integer> listCPU = new ArrayList<>();
                            unavailableActionListCPU.put(functionId, listCPU);
                        }
                    }
                }
            }
        }

        unavailableActionMap.put(CPU_ACTIONS, unavailableActionListCPU);
        unavailableActionMap.put(MEMORY_ACTIONS, unavailableActionListRAM);
        return unavailableActionMap;
    }
}
