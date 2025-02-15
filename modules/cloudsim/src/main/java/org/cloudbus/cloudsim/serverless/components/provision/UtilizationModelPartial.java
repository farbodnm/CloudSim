package org.cloudbus.cloudsim.serverless.components.provision;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;

/**
 * New utilization model class for partial usage of a resource
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */

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
