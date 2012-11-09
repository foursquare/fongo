package com.foursquare.fongo.impl;

import com.mongodb.DBObject;

public interface Filter {
  boolean apply(DBObject o);
}