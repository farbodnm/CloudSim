package org.cloudbus.cloudsim.serverless.components.scheduling;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.Random;

@Slf4j
public class DefaultFunctionScheduler extends FunctionScheduler {

    private int selectedInvokerIndex = 1;

    public DefaultFunctionScheduler() {
        super();
    }

    @Override
    public ContainerVm findVmForContainer(Container container) {
        ServerlessInvoker selectedInvoker = null;
        boolean isInvokerSelected = false;

        switch (Constants.INVOKER_SELECTION_ALGO) {
            case ROUND_ROBIN: {
                for (int i = selectedInvokerIndex; i <= getContainerVmList().size(); i++) {
                    ServerlessInvoker tempSelectedInvoker = ContainerVmList.getById(getContainerVmList(), i);
                    assert tempSelectedInvoker != null;
                    if (tempSelectedInvoker.isSuitableForContainer((ServerlessContainer) container)) {
                        selectedInvoker = tempSelectedInvoker;
                        isInvokerSelected = true;
                        break;
                    }
                }

                if (!isInvokerSelected) {
                    for (int i = 1; i < selectedInvokerIndex; i++) {
                        ServerlessInvoker tempSelectedInvoker = ContainerVmList.getById(getContainerVmList(), i);
                        assert tempSelectedInvoker != null;
                        if (tempSelectedInvoker.isSuitableForContainer((ServerlessContainer) container)) {
                            selectedInvoker = tempSelectedInvoker;
                            break;
                        }
                    }
                }

                assert selectedInvoker != null;
                log.info("{}: {}: Selected Invoker {} for container {}.",
                        CloudSim.clock(), this.getClass().getSimpleName(), selectedInvoker.getId(), container.getId());

                selectedInvokerIndex++;
                selectedInvokerIndex %= getContainerVmList().size();
                break;
            }

            case RANDOM: {
                Random random = new Random();
                while (!isInvokerSelected) {
                    int index = random.nextInt(getContainerVmList().size() - 1);
                    ServerlessInvoker tempSelectedInvoker = ContainerVmList.getById(getContainerVmList(), index);
                    assert tempSelectedInvoker != null;
                    if (tempSelectedInvoker.isSuitableForContainer((ServerlessContainer) container)) {
                        selectedInvoker = tempSelectedInvoker;
                        break;
                    }
                }
                break;
            }

            case BEST_FIT_FIRST: {
                for (int i = 1; i <= getContainerVmList().size(); i++) {
                    ServerlessInvoker tempSelectedInvoker = ContainerVmList.getById(getContainerVmList(), i);
                    assert tempSelectedInvoker != null;
                    if (tempSelectedInvoker.isSuitableForContainer((ServerlessContainer) container)) {
                        selectedInvoker = tempSelectedInvoker;
                        break;
                    }
                }
                break;
            }

            case BEST_FIT_BOUNDED: {
                double minRemainingCap = Double.MAX_VALUE;
                for (int i = 1; i <= getContainerVmList().size(); i++) {
                    ServerlessInvoker tempSelectedInvoker = ContainerVmList.getById(getContainerVmList(), i);
                    assert tempSelectedInvoker != null;
                    double invokerCpuAvailability = tempSelectedInvoker.getAvailableMips() / tempSelectedInvoker.getTotalMips();
                    if (tempSelectedInvoker.isSuitableForContainer((ServerlessContainer) container)) {
                        if (invokerCpuAvailability < minRemainingCap) {
                            minRemainingCap = invokerCpuAvailability;
                            selectedInvoker = tempSelectedInvoker;
                        }
                    }
                }
                break;
            }
        }

        return selectedInvoker;
    }
}
