package org.cloudbus.cloudsim.serverless.components.transfer;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.serverless.components.model.UtilizationModelPartial;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class ServerlessRequest extends ContainerCloudlet {

    private double requestCPUShare = 0D;
    private double requestMemShare = 0D;
    private long containerMIPS = 0;
    private int containerMemory = 0;
    private String functionId = null;

    // TODO: Do we need both?
    private UtilizationModelPartial utilizationModelCPU;
    private UtilizationModelPartial utilizationModelRAM;

    public ServerlessRequest(int requestId, double arrivalTime, String requestFunctionId, long requestLength, int pesNumber,  int containerMemory, long containerMIPS,  double cpuShareReq, double memShareReq, long requestFileSize, long requestOutputSize, UtilizationModelPartial utilizationModelCPU, UtilizationModelPartial utilizationModelRAM, UtilizationModel utilizationModelBw, int retry, boolean success) {
        super(requestId, requestLength, pesNumber, requestFileSize, requestOutputSize, utilizationModelCPU, utilizationModelRAM, utilizationModelBw);
        this.utilizationModelCPU = utilizationModelCPU;
    }

    public double getUtilizationOfCpu() {
        return utilizationModelCPU.getCPUUtilization(this);
    }

    public void setResourceParameter(final int resourceID, final double costPerCPU, final double costPerBw, int vmId) {

    }
}
