package com.foursquare.fongo.impl.geo;

/**
 * Encapsulate external library if we need to remove her.
 */
public class LatLong extends com.github.davidmoten.geo.LatLong {

  public LatLong(double lat, double lon) {
    super(lat, lon);
  }

  public LatLong(com.github.davidmoten.geo.LatLong latLong) {
    super(latLong.getLat(), latLong.getLon());
  }
}
