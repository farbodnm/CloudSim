package org.cloudbus.cloudsim.serverless.enums;

import lombok.extern.slf4j.Slf4j;
import org.cloudbus.cloudsim.serverless.components.ServerlessDatacenter;
import org.cloudbus.cloudsim.serverless.components.autoscale.FunctionAutoScaler;
import org.cloudbus.cloudsim.serverless.components.autoscale.FunctionAutoScalerConstBased;

import java.lang.reflect.Constructor;

@Slf4j
public enum AutoScalerType {

  CONST_BASED(FunctionAutoScalerConstBased.class);

  final Class<? extends FunctionAutoScaler> autoScalerClass;

  AutoScalerType(Class<? extends FunctionAutoScaler> autoScalerClass) {
    this.autoScalerClass = autoScalerClass;
  }

  public Constructor<? extends FunctionAutoScaler> getAutoScalerConstructor() {
    try {
      return autoScalerClass.getConstructor(ServerlessDatacenter.class);
    } catch (NoSuchMethodException ignored) {
      log.error("The provided auto scaler didn't have a constructor that required serverless datacenter.");
      return null;
    }
  }

  public static AutoScalerType getAutoScalerType(Class<? extends FunctionAutoScaler> autoScalerClass) {
    for (AutoScalerType autoScalerType : AutoScalerType.values()) {
      if (autoScalerClass.isInstance(autoScalerType)) {
        return autoScalerType;
      }
    }
    return null;
  }
}
