package org.cloudbus.cloudsim.serverless.components.process;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
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
        return 0.0D;
    }
}
