package org.cloudbus.cloudsim.serverless.components.schedule;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.ServerlessController;
import org.cloudbus.cloudsim.serverless.components.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.utils.Constants;
import org.cloudbus.cloudsim.serverless.utils.IPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

@Slf4j
public class FunctionSchedulerHABIT extends FunctionScheduler {

    public FunctionSchedulerHABIT(ServerlessController controller) {
        super(controller);
    }

    @Override
    public ContainerVm findVmForContainer(Container container) {

        ServerlessInvoker selectedVm = null;
        try {
            int selectedBaseVM = (Integer.parseInt(((ServerlessContainer) container).getFunctionType()) + container.getUserId()) % getContainerVmList().size();
            ServerlessInvoker baseInvoker = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), selectedBaseVM));
            ServerlessInvoker VM1 = baseInvoker;
            ServerlessInvoker VM2 = baseInvoker;

            if (baseInvoker.getCongestionFactor() > 0.1) {
                IPair<Integer, Integer> selectRange = getSelectRange(baseInvoker, selectedBaseVM);

                Random rand = new Random();
                int vm1ID = selectRange.left() + rand.nextInt(selectRange.right() - selectRange.left() + 1);
                int vm2ID = selectRange.left() + rand.nextInt(selectRange.right() - selectRange.left() + 1);
                while (vm2ID == vm1ID && !Objects.equals(selectRange.left(), selectRange.right())) {
                    vm2ID = selectRange.left() + rand.nextInt(selectRange.right() - selectRange.left() + 1);
                }
                if (vm1ID > getContainerVmList().size()) {
                    vm1ID = vm1ID - getContainerVmList().size();
                }
                if (vm2ID > getContainerVmList().size()) {
                    vm2ID = vm2ID - getContainerVmList().size();
                }
                VM1 = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), vm1ID));
                VM2 = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), vm2ID));
            }

            selectedVm = selectBestVMForContainer(VM1, VM2, container);
            if (selectedVm == null) {
                baseInvoker.recordOffload();
                log.info("{}: {}: Container couldn't fit on any vm: {} so congestion updated to: {}",
                        CloudSim.clock(), this.getClass().getSimpleName(),
                        selectedBaseVM, baseInvoker.getCongestionFactor());

                selectedVm = selectInvokerFirstFit(selectedBaseVM, container);
                if (selectedVm == null) {
                    super.controller.resubmitContainerWithDelay(container);
                }
            }

            log.info("{}: {}: Selected vm: {} for container: {} using {} algorithm. Congestion: {}",
                    CloudSim.clock(),
                    this.getClass().getSimpleName(),
                    selectedVm != null ? selectedVm.getId() : "null",
                    container.getId(),
                    Constants.INVOKER_SELECTION_ALGO.name(),
                    selectedVm != null ? selectedVm.getCongestionFactor() : "N/A");
        } catch (Exception e) {
            if (selectedVm != null) {
                log.info(
                        "{}: {}: Selected vm: {} for container: {} using HABIT algorithm but an unexpected exception occurred: {}",
                        CloudSim.clock(),
                        this.getClass().getSimpleName(),
                        selectedVm.getId(),
                        container.getId(),
                        e.getMessage()
                );
            } else {
                e.printStackTrace();
            }
        }

        return selectedVm;
    }

    private IPair<Integer, Integer> getSelectRange(ServerlessInvoker baseInvoker, int selectedBaseVM) {
        double congestionFactor = baseInvoker.getCongestionFactor();
        int rangeSize = (int) Math.min(
                Math.pow(Constants.NUMBER_VMS, Math.pow(congestionFactor, Constants.HABIT_VM_RANGE_EXPONENT)),
                Constants.NUMBER_VMS
        );

        return IPair.of(selectedBaseVM, selectedBaseVM + rangeSize);
    }


    private ServerlessInvoker selectBestVMForContainer(ServerlessInvoker vm1, ServerlessInvoker vm2, Container container) {

        boolean vm1Suitable = vm1.isSuitableForContainer(container);
        boolean vm2Suitable = vm2.isSuitableForContainer(container);

        if (!vm1Suitable) return vm2Suitable ? vm2 : null;
        if (!vm2Suitable) return vm1;

        if (vm1.equals(vm2)) return vm1;

        float vm1RAM = vm1.getContainerRamProvisioner().getAvailableVmRam();
        double vm1MIPS = vm1.getAvailableMips();
        float vm2RAM = vm2.getContainerRamProvisioner().getAvailableVmRam();
        double vm2MIPS = vm2.getAvailableMips();

        int mipsComparison = Double.compare(vm2MIPS, vm1MIPS);
        if (mipsComparison != 0) {
            return mipsComparison > 0 ? vm2 : vm1;
        }

        int ramComparison = Float.compare(vm2RAM, vm1RAM);
        if (ramComparison != 0) {
            return ramComparison > 0 ? vm2 : vm1;
        }

        return vm1;
    }

    private ServerlessInvoker selectInvokerFirstFit(int baseInvoker, Container container) {

        for (int x = baseInvoker; x <= getContainerVmList().size(); x++) {
            ServerlessInvoker tempSelectedVm = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), x));
            if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                return tempSelectedVm;
            }
        }
        for (int x = 1; x < baseInvoker; x++) {
            ServerlessInvoker tempSelectedVm = Objects.requireNonNull(ContainerVmList.getById(getContainerVmList(), x));
            if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                return tempSelectedVm;
            }
        }
        return null;
    }
}
