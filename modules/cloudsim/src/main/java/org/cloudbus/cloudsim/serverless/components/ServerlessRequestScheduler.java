package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.serverless.utils.Constants;

import java.util.ArrayList;
import java.util.List;


/**
 * Request scheduler class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 * <p>
 * todo: fix loggings.
 */
@Slf4j
public class ServerlessRequestScheduler extends ContainerCloudletSchedulerDynamicWorkload {

    @Getter
    private double longestRunTime = 0;

    private double containerQueueTime = 0;

    protected int currentCpus = 0;

    protected int usedPes = 0;

    /**
     * The total current mips requested from each pe by all requests allocated to this container.
     */
    private double totalCurrentRequestedMipsShareForRequests;

    /**
     * The total current mips allocated to all requests running in this container from each pe.
     */
    @Getter
    private double totalCurrentAllocatedMipsShareForRequests;

    /**
     * The total current ram requested by all requests allocated to this container.
     */
    private double totalCurrentRequestedRamForRequests;

    /**
     * The total current ram allocated to all requests running in this container.
     */
    @Getter
    private double totalCurrentAllocatedRamForRequests;

    public ServerlessRequestScheduler(double mips, int numberOfPes) {
        super(mips, numberOfPes);
    }

    public void addToTotalCurrentAllocatedMipsShareForRequests(ServerlessRequest cl) {
        totalCurrentAllocatedMipsShareForRequests += cl.getUtilizationOfCpu();
    }

    public void removeFromTotalCurrentAllocatedMipsShareForRequests(ServerlessRequest cl) {
        totalCurrentAllocatedMipsShareForRequests -= cl.getUtilizationOfCpu();
    }

    public void addToTotalCurrentAllocatedRamForRequests(ServerlessRequest cl) {
        totalCurrentAllocatedRamForRequests += cl.getContainerMemory() * cl.getUtilizationOfRam();
    }

    public void removeFromTotalCurrentAllocatedRamForRequests(ServerlessRequest cl) {
        totalCurrentAllocatedRamForRequests -= cl.getContainerMemory() * cl.getUtilizationOfRam();
    }

    public boolean isSuitableForRequest(ServerlessRequest cl, ServerlessContainer cont) {
        Log.printLine(String.format("Current allocated ram of cont #%s is #%s and requested ram of cl #%s is %s", cont.getId(), totalCurrentAllocatedRamForRequests, cl.getCloudletId(), cl.getContainerMemory() * cl.getUtilizationOfRam()));
        Log.printLine(String.format("Current allocated mips of cont #%s is #%s and requested mips of cl #%s is %s", cont.getId(), totalCurrentAllocatedMipsShareForRequests, cl.getCloudletId(), cl.getUtilizationOfCpu()));
        return (cl.getContainerMemory() * cl.getUtilizationOfRam() <= (cont.getRam() - totalCurrentAllocatedRamForRequests)) && (cl.getNumberOfPes() <= getNumberOfPes()) && (cl.getUtilizationOfCpu() <= 1 - totalCurrentAllocatedMipsShareForRequests);
    }

    /**
     * allocated mips to be calculated using no of pes allocated for
     * the request (not to the entire container) * utilization % of request
     */
    @Override
    public double getEstimatedFinishTime(ResCloudlet rcl, double time) {
        ServerlessRequest cl = (ServerlessRequest) (rcl.getCloudlet());
        return time
                + ((rcl.getRemainingCloudletLength()) / (cl.getNumberOfPes() * this.getMips() * cl.getUtilizationOfCpu()));
    }

    public void deAllocateResources(ServerlessRequest cl) {
        totalCurrentAllocatedRamForRequests -= cl.getContainerMemory() * cl.getUtilizationOfRam();
        totalCurrentAllocatedMipsShareForRequests -= cl.getUtilizationOfCpu();

    }

    /** Is called each time a request is finally submitted to DC */
    public double requestSubmit(ServerlessRequest cl, ServerlessInvoker vm) {
        if (!Constants.CONTAINER_CONCURRENCY || Constants.SCALE_PER_REQUEST) {
            addToTotalCurrentAllocatedMipsShareForRequests(cl);
            addToTotalCurrentAllocatedRamForRequests(cl);
        }
        ResCloudlet rcl = new ResCloudlet(cl);
        rcl.setCloudletStatus(Cloudlet.INEXEC);
        vm.getRunningRequestList().add((ServerlessRequest) cl);
        for (int i = 0; i < cl.getNumberOfPes(); i++) {
            rcl.setMachineAndPeId(0, i);
        }
        getCloudletExecList().add(rcl);
        usedPes += cl.getNumberOfPes();
        vm.addToVmTaskExecutionMap((ServerlessRequest) cl);
        return getEstimatedFinishTime(rcl, getPreviousTime());
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker vm) {
        setCurrentMipsShare(mipsShare);
        int cpus = 0;
        longestRunTime = 0;
        containerQueueTime = 0;

        for (Double mips : mipsShare) { // count the CPUs available to the VMM
            if (mips > 0) {
                cpus++;
            }
        }

        currentCpus = cpus;
        double timeSpan = currentTime - getPreviousTime();
        double nextEvent = Double.MAX_VALUE;
        List<ResCloudlet> requestsToFinish = new ArrayList<>();

        for (ResCloudlet rcl : getCloudletExecList()) {

            rcl.updateCloudletFinishedSoFar((long) (timeSpan
                    * rcl.getCloudlet().getNumberOfPes() * ((ServerlessRequest) (rcl.getCloudlet())).getUtilizationOfCpu() * ((ServerlessRequest) (rcl.getCloudlet())).getContainerMIPS() * Consts.MILLION));

        }

        if (getCloudletExecList().isEmpty() && getCloudletWaitingList().isEmpty()) {
            setPreviousTime(currentTime);
            return 0.0;
        }

        int finished = 0;
        int pesFreed = 0;
        for (ResCloudlet rcl : getCloudletExecList()) {
            // finished anyway, rounding issue...
            if (rcl.getRemainingCloudletLength() == 0) { // finished: remove from the list
                requestsToFinish.add(rcl);
                finished++;
                pesFreed += rcl.getNumberOfPes();
            }
        }
        usedPes -= pesFreed;

        for (ResCloudlet rgl : requestsToFinish) {
            getCloudletExecList().remove(rgl);
            cloudletFinish(rgl);
        }

        List<ResCloudlet> toRemove = new ArrayList<>();
        if (!getCloudletWaitingList().isEmpty()) {
            for (int i = 0; i < finished; i++) {
                toRemove.clear();
                for (ResCloudlet rcl : getCloudletWaitingList()) {
                    if ((currentCpus - usedPes) >= rcl.getNumberOfPes()) {
                        rcl.setCloudletStatus(Cloudlet.INEXEC);
                        boolean added = false;
                        for (int x = 0; x < vm.getRunningRequestList().size(); x++) {
                            if ((((ServerlessRequest) rcl.getCloudlet()).getArrivalTime() + ((ServerlessRequest) rcl.getCloudlet()).getMaxExecTime() <= vm.getRunningRequestList().get(x).getArrivalTime() + vm.getRunningRequestList().get(x).getMaxExecTime())) {
                                vm.getRunningRequestList().add(x, ((ServerlessRequest) rcl.getCloudlet()));
                                added = true;
                                break;
                            }
                        }
                        if (!added) {
                            vm.getRunningRequestList().add((ServerlessRequest) rcl.getCloudlet());
                        }
                        for (int k = 0; k < rcl.getNumberOfPes(); k++) {
                            rcl.setMachineAndPeId(0, i);
                        }
                        getCloudletExecList().add(rcl);

                        // To enable average latency of application
                        vm.addToVmTaskExecutionMap((ServerlessRequest) rcl.getCloudlet());
                        usedPes += rcl.getNumberOfPes();
                        toRemove.add(rcl);
                        break;
                    }
                }
                getCloudletWaitingList().removeAll(toRemove);
            }
        }


        for (ResCloudlet rcl : getCloudletExecList()) {
            double estimatedFinishTime = getEstimatedFinishTime(rcl, currentTime);
            if (estimatedFinishTime < nextEvent) {
                nextEvent = estimatedFinishTime;
            }

            ServerlessRequest task = (ServerlessRequest) (rcl.getCloudlet());
            // Record the longest remaining execution time of the container
            containerQueueTime += task.getMaxExecTime() + task.getArrivalTime() - CloudSim.clock();
            if (task.getMaxExecTime() + task.getArrivalTime() - CloudSim.clock() > longestRunTime) {
                longestRunTime = task.getMaxExecTime() + task.getArrivalTime() - CloudSim.clock();
            }
        }

        for (ResCloudlet rcl : getCloudletWaitingList()) {
            ServerlessRequest task = (ServerlessRequest) (rcl.getCloudlet());
            containerQueueTime += task.getMaxExecTime();
            // Record the longest remaining execution time of the container
            if (task.getMaxExecTime() + task.getArrivalTime() - CloudSim.clock() > longestRunTime) {
                longestRunTime = task.getMaxExecTime() + task.getArrivalTime() - CloudSim.clock();
            }
        }

        setPreviousTime(currentTime);
        requestsToFinish.clear();

        return nextEvent;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public Cloudlet cloudletCancel(int requestId) {
        boolean found = false;
        int position = 0;

        // First, looks in the finished queue
        for (ResCloudlet rcl : getCloudletFinishedList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            return getCloudletFinishedList().remove(position).getCloudlet();
        }

        // Then searches in the exec list
        position = 0;
        for (ResCloudlet rcl : getCloudletExecList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            ResCloudlet rcl = getCloudletExecList().remove(position);
            if (rcl.getRemainingCloudletLength() == 0) {
                cloudletFinish(rcl);
            } else {
                rcl.setCloudletStatus(Cloudlet.CANCELED);
            }
            return rcl.getCloudlet();
        }


        // Then searches in the waiting list
        position = 0;
        for (ResCloudlet rcl : getCloudletWaitingList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            ResCloudlet rcl = getCloudletWaitingList().remove(position);
            if (rcl.getRemainingCloudletLength() == 0) {
                cloudletFinish(rcl);
            } else {
                rcl.setCloudletStatus(Cloudlet.CANCELED);
            }
            return rcl.getCloudlet();
        }

        // Now, looks in the paused queue
        position = 0;
        for (ResCloudlet rcl : getCloudletPausedList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                rcl.setCloudletStatus(Cloudlet.CANCELED);
                break;
            }
            position++;
        }

        if (found) {
            return getCloudletPausedList().remove(position).getCloudlet();
        }

        return null;
    }
}
