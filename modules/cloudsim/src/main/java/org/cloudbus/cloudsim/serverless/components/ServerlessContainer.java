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
 * Created on 3/25/2023
 */

public class ServerlessContainer extends Container {

    /**
     * The running task list for the container
     */
    @Getter
    private final ArrayList<ServerlessRequest> runningTasks = new ArrayList<>();

    /**
     * The running task list for the container
     */
    @Getter
    private final ArrayList<ServerlessRequest> finishedTasks = new ArrayList<>();

    /**
     * Container type
     */
    @Getter
    private final String functionType;

    boolean newContainer;

    @Setter
    private boolean reschedule;

    @Getter
    @Setter
    private boolean idling;

    @Setter
    @Getter
    private double startTime = 0;

    @Setter
    @Getter
    private double finishTime = 0;

    @Setter
    @Getter
    private double idleStartTime = 0;

    public ServerlessContainer(
        int id,
        int userId,
        String type,
        double mips,
        int numberOfPes,
        int ram,
        long bw,
        long size,
        String containerManager,
        ContainerCloudletScheduler containerRequestScheduler,
        double schedulingInterval,
        boolean newCont,
        boolean idling,
        boolean reschedule,
        double idleStartTime
    ) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerRequestScheduler, schedulingInterval);
        this.newContainer = newCont;
        this.reschedule = reschedule;
        this.idling = idling;
        this.functionType = type;
        this.idleStartTime = idleStartTime;
    }

    public void addToRunningTasks(ServerlessRequest task) {
        runningTasks.add(task);
    }

    public void addToFinishedTasks(ServerlessRequest task) {
        finishedTasks.add(task);
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker vm) {
        if (mipsShare != null) {
            return ((ServerlessRequestScheduler) getContainerCloudletScheduler()).updateContainerProcessing(currentTime, mipsShare,vm);
        }
        return 0.0;
    }
}
