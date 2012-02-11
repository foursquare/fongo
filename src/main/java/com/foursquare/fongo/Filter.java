package com.foursquare.fongo;

import com.mongodb.DBObject;

public interface Filter {
  boolean apply(DBObject o);
}