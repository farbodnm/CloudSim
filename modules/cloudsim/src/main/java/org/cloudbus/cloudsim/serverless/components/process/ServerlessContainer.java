package org.cloudbus.cloudsim.serverless.components.process;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessRequestScheduler;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ServerlessContainer extends Container {

    public boolean newContainer;

    private boolean firstProcess = false;
    private double idleStartTime;
    private double finishTime = 0;
    private boolean reschedule;
    private boolean idling;
    // TODO: Maybe use enum?
    private  String functionType;

    private List<ServerlessRequest> pendingTasks = new ArrayList<>();
    private List<ServerlessRequest> runningTasks = new ArrayList<>();
    private List<ServerlessRequest> finishedTasks = new ArrayList<>();

    public ServerlessContainer(
            int id,
            int userId,
            double mips,
            String functionType,
            int numberOfPes,
            int ram,
            long bw,
            long size,
            String containerManager,
            ContainerCloudletScheduler containerCloudletScheduler,
            double schedulingInterval,
            boolean newContainer,
            boolean idling,
            boolean reschedule,
            double idleStartTime
    ) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerCloudletScheduler, schedulingInterval);
        this.newContainer = newContainer;
        this.reschedule = reschedule;
        this.idling = idling;
        this.functionType = functionType;
        this.idleStartTime = idleStartTime;
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker invoker) {
        return 0.0D;
    }
}
