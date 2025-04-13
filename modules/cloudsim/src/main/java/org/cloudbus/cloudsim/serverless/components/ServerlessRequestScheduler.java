package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.enums.ResCloudletList;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@Setter
@Slf4j
public class ServerlessRequestScheduler extends ContainerCloudletSchedulerDynamicWorkload {

    private double longestContainerRunTime = 0;
    private double containerQueueTime = 0;
    private double totalCurrentAllocatedMipsShareForRequests;
    private double totalCurrentAllocatedRamForRequests;
    private int usedPes = 0;

    protected int currentCPUs = 0;

    public ServerlessRequestScheduler(double mips, int numberOfPes) {
        super(mips, numberOfPes);
    }

    /**
     * Overrides
     */

    @Override
    public double getEstimatedFinishTime(ResCloudlet resCloudlet, double time) {

        ServerlessRequest request = (ServerlessRequest) resCloudlet.getCloudlet();
        return time + resCloudlet.getRemainingCloudletLength() / (request.getNumberOfPes() * this.getMips() * request.getUtilizationOfCpu());
    }

    public Cloudlet cloudletCancel(int requestId) {

        int position = getRequestPositionInList(requestId, ResCloudletList.FINISHED);
        if (position != -1) {
            return getCloudletFinishedList().remove(position).getCloudlet();
        }

        position = getRequestPositionInList(requestId, ResCloudletList.EXEC);
        if (position != -1) {
            ResCloudlet resCloudlet = getCloudletExecList().remove(position);
            if (resCloudlet.getRemainingCloudletLength() == 0) {
                cloudletFinish(resCloudlet);
            } else {
                resCloudlet.setCloudletStatus(Cloudlet.CANCELED);
            }
            return resCloudlet.getCloudlet();
        }

        position = getRequestPositionInList(requestId, ResCloudletList.WAITING);
        if (position != -1) {
            ResCloudlet resCloudlet = getCloudletWaitingList().remove(position);
            if (resCloudlet.getRemainingCloudletLength() == 0) {
                cloudletFinish(resCloudlet);
            } else {
                resCloudlet.setCloudletStatus(Cloudlet.CANCELED);
            }
            return resCloudlet.getCloudlet();
        }

        position = getRequestPositionInList(requestId, ResCloudletList.PAUSED);
        if (position != -1) {
            ResCloudlet resCloudlet = getCloudletPausedList().remove(position);
            resCloudlet.setCloudletStatus(Cloudlet.CANCELED);
            return resCloudlet.getCloudlet();
        }

        return null;
    }

    /**
     * Public functionalities
     */

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker invoker) {

        setCurrentMipsShare(mipsShare);
        int cpus = 0;
        longestContainerRunTime = 0;
        containerQueueTime = 0;
        for (double mips: mipsShare) {
            if (mips > 0) {
                cpus++;
            }
        }
        currentCPUs = cpus;

        double timeSpan = currentTime - getPreviousTime();
        double nextEvent = Double.MAX_VALUE;
        List<ResCloudlet> requestsToFinish = new ArrayList<>();
        for (ResCloudlet resCloudlet: getCloudletExecList()) {
            resCloudlet.updateCloudletFinishedSoFar(
                    (long) (
                            timeSpan * resCloudlet.getCloudlet().getNumberOfPes()
                                    * ((ServerlessRequest) resCloudlet.getCloudlet()).getUtilizationOfCpu()
                                    * ((ServerlessRequest) resCloudlet.getCloudlet()).getContainerMIPS()
                                    * Consts.MILLION
                    )
            );
        }
        if (getCloudletExecList().isEmpty() && getCloudletWaitingList().isEmpty()) {
            setPreviousTime(currentTime);
            return 0D;
        }

        int finished = 0;
        int pesFreed = 0;
        for (ResCloudlet resCloudlet: getCloudletExecList()) {
            if (resCloudlet.getRemainingCloudletLength() == 0) {
                requestsToFinish.add(resCloudlet);
                finished++;
                pesFreed += resCloudlet.getNumberOfPes();
            }
        }
        usedPes -= pesFreed;

        for (ResCloudlet resCloudlet: requestsToFinish) {
            getCloudletExecList().remove(resCloudlet);
            cloudletFinish(resCloudlet);
        }

        List<ResCloudlet> toRemove = new ArrayList<>();
        if (!getCloudletWaitingList().isEmpty()) {
            for (int i = 0; i < finished; i++) {
                toRemove.clear();
                for (ResCloudlet resCloudlet: getCloudletWaitingList()) {
                    if ((currentCPUs - usedPes) >= resCloudlet.getNumberOfPes()) {
                        resCloudlet.setCloudletStatus(Cloudlet.INEXEC);
                        boolean added = false;
                        ServerlessRequest request = (ServerlessRequest) resCloudlet.getCloudlet();
                        for (int x = 0; x < invoker.getRunningRequestsList().size(); x++) {
                            if (request.getArrivalTime() + request.getMaxExecTime()
                                    <= invoker.getRunningRequestsList().get(x).getArrivalTime()
                                    + invoker.getRunningRequestsList().get(x).getMaxExecTime()) {
                                invoker.getRunningRequestsList().add(x, request);
                                added = true;
                                break;
                            }
                        }

                        if (!added) {
                            invoker.getRunningRequestsList().add(request);
                        }

                        for (int x = 0; x < resCloudlet.getNumberOfPes(); x++) {
                            resCloudlet.setMachineAndPeId(0, i);
                        }
                        getCloudletExecList().add(resCloudlet);

                        invoker.addToRequestExecutionMap(request);
                        usedPes += resCloudlet.getNumberOfPes();
                        toRemove.add(resCloudlet);
                        break;
                    }
                }
                getCloudletWaitingList().removeAll(toRemove);
            }
        }

        for (ResCloudlet resCloudlet: getCloudletExecList()) {
            double estimatedFinishTime = getEstimatedFinishTime(resCloudlet, currentTime);
            if (estimatedFinishTime < nextEvent) {
                nextEvent = estimatedFinishTime;
            }

            ServerlessRequest request = (ServerlessRequest) resCloudlet.getCloudlet();
            containerQueueTime += request.getMaxExecTime() + request.getArrivalTime() - CloudSim.clock();
            if (request.getMaxExecTime() + request.getArrivalTime() - CloudSim.clock() > longestContainerRunTime) {
                longestContainerRunTime = request.getArrivalTime() + request.getArrivalTime() - CloudSim.clock();
            }
        }

        for (ResCloudlet resCloudlet: getCloudletWaitingList()) {
            ServerlessRequest request = (ServerlessRequest) resCloudlet.getCloudlet();
            containerQueueTime += request.getMaxExecTime();
            if (request.getMaxExecTime() + request.getArrivalTime() - CloudSim.clock() > longestContainerRunTime) {
                longestContainerRunTime = request.getArrivalTime() + request.getArrivalTime() - CloudSim.clock();
            }
        }

        setPreviousTime(currentTime);
        requestsToFinish.clear();
        return nextEvent;
    }

    public double requestSubmit(ServerlessRequest request, ServerlessInvoker invoker, ServerlessContainer contaienr) {
        if (!Constants.CONTAINER_CONCURRENCY || Constants.SCALE_PER_REQUEST) {
            addToTotalCurrentAllocatedCpuForRequests(request);
            addToTotalCurrentAllocatedCpuForRequests(request);
        }
        ResCloudlet resCloudlet = new ResCloudlet(request);
        resCloudlet.setCloudletStatus(Cloudlet.INEXEC);
        invoker.getRunningRequestsList().add(request);
        for (int i = 0; i < request.getNumberOfPes(); i++) {
            resCloudlet.setMachineAndPeId(0, i);
        }
        getCloudletExecList().add(resCloudlet);
        usedPes += request.getNumberOfPes();
        invoker.addToRequestExecutionMap(request);
        return getEstimatedFinishTime(resCloudlet, getPreviousTime());
    }

    public boolean isSuitableForRequest(ServerlessRequest request, ServerlessContainer container) {
        log.info("{}: {}: Ram requested for container: {} is: {} while currently allocated ram is: {}",
                CloudSim.clock(), "RequestScheduler", container.getId(), request.getContainerMemory() * request.getUtilizationOfRam(), totalCurrentAllocatedRamForRequests);
        log.info("{}: {}: Cpu requested for container: {} is: {} while currently allocated cpu is: {}",
                CloudSim.clock(), "RequestScheduler", container.getId(), request.getUtilizationOfCpu(), totalCurrentAllocatedMipsShareForRequests);
        return request.getContainerMemory() * request.getUtilizationOfRam() <= container.getRam() - totalCurrentAllocatedRamForRequests
                && request.getNumberOfPes() <= getNumberOfPes()
                && request.getUtilizationOfCpu() <= 1 - totalCurrentAllocatedMipsShareForRequests;
    }

    public void addToTotalCurrentAllocatedRamForRequests(ServerlessRequest request) {
        totalCurrentAllocatedRamForRequests += request.getContainerMemory() * request.getUtilizationOfRam();
    }

    public void addToTotalCurrentAllocatedCpuForRequests(ServerlessRequest request) {
        totalCurrentAllocatedMipsShareForRequests += request.getUtilizationOfCpu();
    }

    public void deAllocateResources(ServerlessRequest request) {
        totalCurrentAllocatedRamForRequests -= request.getContainerMemory() * request.getUtilizationOfRam();
        totalCurrentAllocatedMipsShareForRequests -= request.getUtilizationOfCpu();
    }

    /**
     * Local functionalities
     */

    private int getRequestPositionInList(int requestId, ResCloudletList resCloudletListType) {
        List<? extends ResCloudlet> resCloudlets = Collections.emptyList();
        switch (resCloudletListType) {
            case FINISHED:
                resCloudlets = getCloudletFinishedList();
                break;
            case EXEC:
                resCloudlets = getCloudletExecList();
                break;
            case WAITING:
                resCloudlets = getCloudletWaitingList();
                break;
            case PAUSED:
                resCloudlets = getCloudletPausedList();
                break;
        }

        int position = 0;
        for (ResCloudlet resCloudlet: resCloudlets) {
            if (resCloudlet.getCloudletId() == requestId) {
                return position;
            }
            position++;
        }
        return -1;
    }
}
