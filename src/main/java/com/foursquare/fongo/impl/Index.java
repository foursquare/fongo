package com.foursquare.fongo.impl;

import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.List;

/**
 * An index for the MongoDB.
 * Must be immutable.
 */
public class Index {
  private final String name;
  private final List<String> fields;
  private final boolean unique;

  public Index(String name, List<String> fields, boolean unique) {
    this.name = name;
    this.fields = fields;
    this.unique = unique;
  }

  public Index(String name, DBObject keys, boolean unique) {
    this(name, new ArrayList<String>(keys.keySet()), unique);
  }

  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  public List<String> getFields() {
    return fields;
  }
}
