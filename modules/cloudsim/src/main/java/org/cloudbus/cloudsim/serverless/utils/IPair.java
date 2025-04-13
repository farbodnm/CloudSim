package org.cloudbus.cloudsim.serverless.utils;

public final class IPair<T, H> {

  private final T left;
  private final H right;

  public IPair(T left, H right) {
    this.left = left;
    this.right = right;
  }

  public static <T, H> IPair<T, H> of(T right, H left) {
    return new IPair<>(right, left);
  }

  public T left() {
    return left;
  }

  public H right() {
    return right;
  }
}
