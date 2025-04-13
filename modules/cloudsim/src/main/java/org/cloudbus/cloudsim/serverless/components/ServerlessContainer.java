package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * Container class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

@Getter
@Setter
public class ServerlessContainer extends Container {

    public boolean newContainer;

    private boolean firstProcess = false;
    private double startTime;
    private double idleStartTime;
    private double finishTime = 0;
    private boolean reschedule;
    private boolean idling;
    private String functionType;

    private List<ServerlessRequest> pendingRequests = new ArrayList<>();
    private List<ServerlessRequest> runningRequests = new ArrayList<>();
    private List<ServerlessRequest> finishedRequests = new ArrayList<>();

    public ServerlessContainer(
            int id,
            int controllerId,
            String functionType,
            double mips,
            int numberOfPes,
            int ram,
            long bw,
            long size,
            String containerManager,
            ContainerCloudletScheduler containerRequestScheduler,
            double schedulingInterval
    ) {
        super(id, controllerId, mips, numberOfPes, ram, bw, size, containerManager, containerRequestScheduler, schedulingInterval);
        this.functionType = functionType;
        this.newContainer = true;
        this.reschedule = false;
        this.idling = false;
        this.idleStartTime = 0;
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker invoker) {
        if (mipsShare != null) {
            return ((ServerlessRequestScheduler) getContainerCloudletScheduler()).updateContainerProcessing(currentTime, mipsShare, invoker);
        }
        return 0D;
    }

    public void addToRunningRequests(ServerlessRequest request) {
        runningRequests.add(request);
    }

    public void removeFromRunningRequests(ServerlessRequest request) {
        runningRequests.remove(request);
    }

    public void addToFinishedRequests(ServerlessRequest request) {
        finishedRequests.add(request);
    }
}
