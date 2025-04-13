package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class ServerlessRequest extends ContainerCloudlet {

    private double requestCPUShare = 0D;
    private double requestMemShare = 0D;
    private double maxExecTime = 0D;
    private long containerMIPS = 0;
    private int containerMemory = 0;
    private int retry;
    private boolean success;
    private double arrivalTime = 0D;
    private int priority = 0;
    private String functionId = null;
    private double cpuShare = 0D;
    private double memShare = 0D;

    private UtilizationModelPartial utilizationModelCPU;
    private UtilizationModelPartial utilizationModelRAM;

    public ServerlessRequest(int requestId, double arrivalTime, String requestFunctionId, long requestLength, int pesNumber,  int containerMemory, long containerMIPS,  double cpuShare, double memShare, long requestFileSize, long requestOutputSize, UtilizationModelPartial utilizationModelCPU, UtilizationModelPartial utilizationModelRAM, UtilizationModel utilizationModelBw, int retry, boolean success) {
        super(requestId, requestLength, pesNumber, requestFileSize, requestOutputSize, utilizationModelCPU, utilizationModelRAM, utilizationModelBw);
        this.retry = retry;
        this.success = success;
        this.functionId = requestFunctionId;
        this.containerMemory = containerMemory;
        this.containerMIPS = containerMIPS;
        this.arrivalTime = arrivalTime;
        this.utilizationModelCPU = utilizationModelCPU;
        this.utilizationModelRAM = utilizationModelRAM;
        this.cpuShare = cpuShare;
        this.memShare = memShare;
    }

    public double getUtilizationOfCpu() {
        return utilizationModelCPU.getCPUUtilization(this);
    }

    public double getUtilizationOfRam() {
        return utilizationModelRAM.getMemUtilization(this);
    }

    /**
     * Datacenter functionalities
     */

    public void setResourceParameter(final int resourceID, final double costPerCPU, final double costPerBw, int vmId) {

        final Resource res = new Resource();
        res.vmId = vmId;
        res.resourceId = resourceID;
        res.costPerSec = costPerCPU;
        this.costPerBw = costPerBw;
        res.resourceName = CloudSim.getEntityName(resourceID);
        resList.add(res);
        accumulatedBwCost = costPerBw * getCloudletFileSize();

        if (index == -1 && record) {
            write("Allocates this request to " + res.resourceName + " (ID #" + resourceID
                    + ") with cost = $" + costPerCPU + "/sec");
        } else if (record) {
            final int id = resList.get(index).resourceId;
            final String name = resList.get(index).resourceName;
            write("Moves request from " + name + " (ID #" + id + ") to " + res.resourceName + " (ID #"
                    + resourceID + ") with cost = $" + costPerCPU + "/sec");
        }

        index++;
    }

    /**
     * Public functionalities
     */

    public void incrementRetryCount() {
        retry++;
    }

    /**
     * Test functionalities
     */

    public String getResList() {

        List<String> resStringList = new ArrayList<>();
        for (Resource res: resList) {
            resStringList.add(String.valueOf(res.vmId));
        }
        return String.join(", ", resStringList);
    }
}
