package org.cloudbus.cloudsim.serverless.components;

import lombok.Getter;
import lombok.Setter;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.serverless.utils.Constants;

public class CongestionTracker {

    @Getter
    private double shortTermEMA = 0;

    @Getter
    private double mediumTermEMA = 0;

    @Getter
    private double previousTime = 0;

    private double lastBusyTime = CloudSim.clock();

    public void update(boolean hadOffloads, boolean isCurrentlyEmpty) {

        if (!hadOffloads) {
            previousTime = CloudSim.clock();
        }

        shortTermEMA = Constants.SHORT_TERM_ALPHA * (hadOffloads ? 1 : 0)
                + (1 - Constants.SHORT_TERM_ALPHA) * shortTermEMA;
        mediumTermEMA = Constants.MEDIUM_TERM_ALPHA * (hadOffloads ? 1 : 0)
                + (1 - Constants.MEDIUM_TERM_ALPHA) * mediumTermEMA;

        if (!isCurrentlyEmpty) {
            lastBusyTime = CloudSim.clock();
        }
    }

    public double getCongestionFactor() {
        double emptyDuration = CloudSim.clock() - lastBusyTime;
        double emptinessFactor = Math.exp(-emptyDuration / Constants.DECAY_HALF_LIFE);
        return (shortTermEMA * 0.3 + mediumTermEMA * 0.7) * emptinessFactor;
    }
}
