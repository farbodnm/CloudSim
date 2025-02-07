package org.cloudbus.cloudsim.serverless.components.scaling;

import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessContainerScheduler;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.*;

/**
 * The default container scaler that is based on constants
 *
 * @see org.cloudbus.cloudsim.serverless.utils.Constants
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
            List<? extends ContainerHost> invokerList = getDatacenter().getVmAllocationPolicy().getContainerHostList();
            Map<String, Map<String, List<Integer>>> unAvailableActionMap = containerVerticalAutoScaler();
            int cpuIncrement = 0;
            int memIncrement = 0;
            double cpuUtilization = 0D;
            String scalingFunction = null;

            for (Map.Entry<String, Map<String, Double>> data : functionData.getKey().entrySet()) {
            }
        }
    }

    @Override
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
                                if (container.getRunningTasks().isEmpty()) {
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

    @Override
    protected void containerHorizontalScaler(Map<String, Map<String, Double>> fnNestedMap, Map<String, List<ServerlessContainer>> emptyContainers) {

    }

    @Override
    protected Map<String, Map<String, List<Integer>>> containerVerticalAutoScaler() {
        return Collections.emptyMap();
    }
}
