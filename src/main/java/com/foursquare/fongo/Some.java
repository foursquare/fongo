package com.foursquare.fongo;

class Some<T> implements Option<T> {

  private final T value;

  public Some(T value) {
    this.value = value;
  }
  @Override
  public T get() {
    return value;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }
  @Override
  public boolean isFull() {
    return true;
  }
  @Override
  public String toString() {
    return "Some("+String.valueOf(value)+")";
  }
}