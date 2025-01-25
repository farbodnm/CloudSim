package org.cloudbus.cloudsim.serverless.components.process;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.enums.InvokerStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ServerlessInvoker extends PowerContainerVm {

    public double onTime = 0;
    public double offTime = 0;

    private  double recordTime = 0; // TODO: ?
    private boolean used;
    private InvokerStatus status = null;

    private Map<String, List<ServerlessContainer>> pendingFunctionContainerMap = new HashMap<>();
    private List<ServerlessRequest> runningRequestsList = new ArrayList<>();

    public ServerlessInvoker(int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    }

    public void addToPendingFunctionContainerMap(ServerlessContainer container, String functionId) {

    }
}
