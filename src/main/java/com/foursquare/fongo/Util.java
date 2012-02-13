package com.foursquare.fongo;

import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBList;

public class Util {
  public static <T> BasicDBList list(T ... ts){
    return wrap(Arrays.asList(ts));
  }
  
  public static BasicDBList wrap(List otherList) {
    BasicDBList list = new BasicDBList();
    list.addAll(otherList);
    return list;
  }
}
