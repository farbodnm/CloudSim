package org.cloudbus.cloudsim.serverless.utils;

import org.cloudbus.cloudsim.core.CloudSimTags;

public class CloudSimSCTags extends CloudSimTags {

    private static final int BASE = 0;
    public static final int DEADLINE_CHECKPOINT = BASE + 51;
    public static final int CLOUDLET_RESCHEDULE = 52;
    public static final int RECORD_CPU_USAGE = 53;
    public static final int CREATE_CLOUDLETS = 54;
    public static final int PREEMPT_REQUEST = 55 ;
    public static final int SCALED_CONTAINER = 56 ;
    public static final int AUTO_SCALE = 57 ;
    public static final int VERTICAL_SCALE = 58 ;

    private CloudSimSCTags() {
        super();
    }
}
