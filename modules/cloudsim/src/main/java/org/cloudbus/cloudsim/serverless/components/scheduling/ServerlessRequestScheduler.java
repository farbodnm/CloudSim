package org.cloudbus.cloudsim.serverless.components.scheduling;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessContainer;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServerlessRequestScheduler extends ContainerCloudletSchedulerDynamicWorkload {

    private double longestContainerRunTime = 0;
    private double containerQueueTime = 0;

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
}
