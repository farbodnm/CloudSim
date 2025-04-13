package org.cloudbus.cloudsim.serverless.components.loadbalance;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.serverless.components.ServerlessController;
import org.cloudbus.cloudsim.serverless.components.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.ServerlessRequest;

/**
 * Loadbalancer class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
@Getter
@Setter
public abstract class RequestLoadBalancer {

    protected ServerlessController controller;
    protected ServerlessDatacenter datacenter;

    public RequestLoadBalancer(ServerlessController controller, ServerlessDatacenter datacenter) {
        this.controller = controller;
        this.datacenter = datacenter;
    }

    /**
     *  Controller functionalities
     */

    public abstract void routeRequest(ServerlessRequest request);
}
