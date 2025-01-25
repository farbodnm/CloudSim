package org.cloudbus.cloudsim.serverless.components.model;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;

public class UtilizationModelPartial implements UtilizationModel {

    @Override
    public double getUtilization(double time) {
        return 0;
    }

    public double getCPUUtilization(ServerlessRequest request) {
        return request.getRequestCPUShare();
    }

    public double getMemUtilization(ServerlessRequest request) {
        return request.getRequestMemShare();
    }
}
