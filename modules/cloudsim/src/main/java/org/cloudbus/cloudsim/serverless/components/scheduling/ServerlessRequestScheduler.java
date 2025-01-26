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

    @Override
    public double updateContainerProcessing(double currentTime, List<Double> mipsShare) {
        return 0;
    }

    @Override
    public double cloudletSubmit(Cloudlet gl, double fileTransferTime) {
        return 0;
    }

    @Override
    public double cloudletSubmit(Cloudlet gl) {
        return 0;
    }

    @Override
    public Cloudlet cloudletCancel(int clId) {
        return null;
    }

    @Override
    public boolean cloudletPause(int clId) {
        return false;
    }

    @Override
    public double cloudletResume(int clId) {
        return 0;
    }

    @Override
    public void cloudletFinish(ResCloudlet rcl) {

    }

    @Override
    public int getCloudletStatus(int clId) {
        return 0;
    }

    @Override
    public boolean isFinishedCloudlets() {
        return false;
    }

    @Override
    public Cloudlet getNextFinishedCloudlet() {
        return null;
    }

    @Override
    public int runningCloudlets() {
        return 0;
    }

    @Override
    public Cloudlet migrateCloudlet() {
        return null;
    }

    @Override
    public double getTotalUtilizationOfCpu(double time) {
        return 0;
    }

    @Override
    public List<Double> getCurrentRequestedMips() {
        return Collections.emptyList();
    }

    @Override
    public double getTotalCurrentAvailableMipsForCloudlet(ResCloudlet rcl, List<Double> mipsShare) {
        return 0;
    }

    @Override
    public double getTotalCurrentRequestedMipsForCloudlet(ResCloudlet rcl, double time) {
        return 0;
    }

    @Override
    public double getTotalCurrentAllocatedMipsForCloudlet(ResCloudlet rcl, double time) {
        return 0;
    }

    @Override
    public double getCurrentRequestedUtilizationOfRam() {
        return 0;
    }

    @Override
    public double getCurrentRequestedUtilizationOfBw() {
        return 0;
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker invoker) {
        return 0.0D;
    }

    public double requestSubmit(ServerlessRequest request, ServerlessInvoker invoker, ServerlessContainer contaienr) {
        return 0.0D;
    }
}
