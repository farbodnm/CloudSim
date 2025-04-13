package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * Serverless request class for CloudSimSC extension. This class represents a single user request
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 * TODO: max exec time requires some fixing.
 */

public class ServerlessRequest extends ContainerCloudlet {

    @Getter
    private final String requestFunctionId;

    @Getter
    private final int containerMemory;

    @Getter
    private final long containerMIPS;

    @Getter
    private final double maxExecTime = 0;

    @Getter
    private final double arrivalTime;

    @Setter
    public boolean success;

    public int retry;
    private final double cpuShareRequest;
    private final double memShareRequest;

    public ServerlessRequest(int requestId, double arrivalTime, String requestFunctionId, long requestLength, int pesNumber,  int containerMemory, long containerMIPS,  double cpuShareReq, double memShareReq, long requestFileSize, long requestOutputSize, UtilizationModelPartial utilizationModelCpu, UtilizationModelPartial utilizationModelRam, UtilizationModel utilizationModelBw, int retry, boolean success) {
        super(requestId, requestLength, pesNumber, requestFileSize, requestOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);

        this.requestFunctionId = requestFunctionId;
        this.containerMemory = containerMemory;
        this.containerMIPS = containerMIPS;
        this.arrivalTime = arrivalTime;
        this.success = success;
        this.retry = retry;
        this.cpuShareRequest = cpuShareReq;
        this.memShareRequest = memShareReq;
        super.setUtilizationModelRam(utilizationModelCpu);
        super.setUtilizationModelCpu(utilizationModelCpu);
    }

    public void setResourceParameter(final int resourceID, final double cost, int vmId) {
        final Resource res = new Resource();
        res.vmId = vmId;
        res.resourceId = resourceID;
        res.costPerSec = cost;
        res.resourceName = CloudSim.getEntityName(resourceID);

        // add into a list if moving to a new grid resource
        resList.add(res);

        if (index == -1 && record) {
            write("Allocates this request to " + res.resourceName + " (ID #" + resourceID
                    + ") with cost = $" + cost + "/sec");
        } else if (record) {
            final int id = resList.get(index).resourceId;
            final String name = resList.get(index).resourceName;
            write("Moves request from " + name + " (ID #" + id + ") to " + res.resourceName + " (ID #"
                    + resourceID + ") with cost = $" + cost + "/sec");
        }

        index++;  // initially, index = -1
    }


    public void setResourceParameter(final int resourceID, final double costPerCPU, final double costPerBw, int vmId) {
        setResourceParameter(resourceID, costPerCPU, vmId);
        this.costPerBw = costPerBw;
        accumulatedBwCost = costPerBw * getCloudletFileSize();
    }

    public String getResList() {
        String resString = "";
        for(int x=0; x<resList.size(); x++){
            if(x==resList.size()-1){
                resString = resString.concat(Integer.toString(resList.get(x).vmId)) ;
            }
            else
                resString = resString.concat(resList.get(x).vmId +" ,") ;

        }
        return resString;
    }

    public double getUtilizationOfCpu() {
        return cpuShareRequest;
    }

    public double getUtilizationOfRam() {
        return memShareRequest;
    }
}

