package com.foursquare.fongo;

interface Option<T> {
  static final Option None = new Option(){

    @Override
    public Object get() {
      throw new RuntimeException("getting None option");
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean isFull() {
      return false;
    }};
  
  
  T get();
  boolean isEmpty();
  boolean isFull();
}