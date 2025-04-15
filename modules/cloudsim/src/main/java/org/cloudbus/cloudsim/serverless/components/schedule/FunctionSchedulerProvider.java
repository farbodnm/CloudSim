package org.cloudbus.cloudsim.serverless.components.schedule;

import org.cloudbus.cloudsim.serverless.components.ServerlessController;
import org.cloudbus.cloudsim.serverless.utils.Constants;

public final class FunctionSchedulerProvider {
    public static FunctionScheduler getScheduler(ServerlessController controller) {
        switch (Constants.INVOKER_SELECTION_ALGO) {
            case HABIT:
                return new FunctionSchedulerHABIT(controller);
            default:
                return new FunctionSchedulerConstBased(controller);
        }
    }
}
