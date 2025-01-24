package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;

public class ServerlessContainer extends Container {

    @Getter
    @Setter
    private boolean firstProcess = false;
    @Getter
    private double idleStartTime = 0;
    @Setter
    private double finishTime = 0;

    public ServerlessContainer(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String containerManager, ContainerCloudletScheduler containerCloudletScheduler, double schedulingInterval) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerCloudletScheduler, schedulingInterval);
    }

    public void startIdleTime() {
        idleStartTime = 0;
    }
}
