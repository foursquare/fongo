package com.foursquare.fongo;

/**
 * Internal fongo exception
 * @author jon
 *
 */
public class FongoException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public FongoException(String message) {
    super(message);
  }
}
