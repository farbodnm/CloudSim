package org.cloudbus.cloudsim.serverless.components.loadbalancer;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.process.ServerlessInvoker;
import org.cloudbus.cloudsim.serverless.components.scheduling.ServerlessController;
import org.cloudbus.cloudsim.serverless.components.transfer.ServerlessRequest;
import org.cloudbus.cloudsim.serverless.utils.Constants;

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
