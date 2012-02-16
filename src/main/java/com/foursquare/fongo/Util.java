package com.foursquare.fongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  public static List<String> split(String key){
    char dot = '.';
    int index = key.indexOf(dot);
    if (index <= 0) {
      return Collections.singletonList(key);
    } else {
      ArrayList<String> path = new ArrayList<String>(5);
      while ( index > 0){
        path.add(key.substring(0, index));
        key = key.substring(index + 1);
        index = key.indexOf(dot);
      }
      path.add(key);
      return path;
    }
  }
}
