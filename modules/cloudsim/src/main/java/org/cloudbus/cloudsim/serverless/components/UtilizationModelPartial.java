package org.cloudbus.cloudsim.serverless.components;

import org.cloudbus.cloudsim.UtilizationModel;

/**
 * New utilization model class for partial usage of a resource
 *
 * @author Anupama Mampage
 */

public class UtilizationModelPartial implements UtilizationModel {
    @Override
    public double getUtilization(double time) {
        return 0;
    }
}
