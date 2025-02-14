package org.cloudbus.cloudsim.serverless.components.scheduling;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;

import java.util.List;

@Getter
@Setter
public class ServerlessRequestScheduler extends ContainerCloudletSchedulerDynamicWorkload {

    private double longestContainerRunTime = 0;
    private double containerQueueTime = 0;
    private double totalCurrentAllocatedMipsShareForRequests;
    private double totalCurrentAllocatedRamForRequests;

    protected int currentCPUs = 0;

    public ServerlessRequestScheduler(double mips, int numberOfPes) {
        super(mips, numberOfPes);
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker invoker) {
        return 0.0D;
    }

    public double requestSubmit(ServerlessRequest request, ServerlessInvoker invoker, ServerlessContainer contaienr) {
        return 0.0D;
    }

    public void deAllocateResources(ServerlessRequest request) {

    }

    public boolean isSuitableForRequest(ServerlessRequest request, ServerlessContainer contaienr) {
        return true;
    }

    public void addToTotalCurrentAllocatedRamForRequests(ServerlessRequest request) {
        totalCurrentAllocatedRamForRequests += request.getContainerMemory() * request.getUtilizationOfRam();
    }

    public void addToTotalCurrentAllocatedCpuForRequests(ServerlessRequest request) {
        totalCurrentAllocatedMipsShareForRequests += request.getUtilizationOfCpu();
    }
}
