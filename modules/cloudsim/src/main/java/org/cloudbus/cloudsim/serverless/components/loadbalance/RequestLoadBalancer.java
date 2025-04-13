package org.cloudbus.cloudsim.serverless.components.loadbalance;

import org.cloudbus.cloudsim.serverless.components.ServerlessRequest;


/**
 * Loadbalancer class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * @author Farbod Nazari
 */
public interface RequestLoadBalancer {

    void routeRequest(ServerlessRequest request);
}
