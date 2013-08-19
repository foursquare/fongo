package com.foursquare.fongo;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.junit.rules.ExpectedException;

public final class ExpectedMongoException {

  private ExpectedMongoException() {
  }

  public static ExpectedException expectCommandFailure(ExpectedException expectedExcpetion, int code) {
    expect(expectedExcpetion, CommandFailureException.class);
    expectedExcpetion.expect(equalCode(code));
    return expectedExcpetion;
  }

  public static ExpectedException expect(ExpectedException expectedExcpetion, Class<? extends MongoException> exception) {
    expectedExcpetion.expect(exception);
    return expectedExcpetion;
  }

  public static ExpectedException expectCode(ExpectedException expectedExcpetion, int code) {
    expectedExcpetion.expect(equalCode(code));
    return expectedExcpetion;
  }

  public static ExpectedException expectCode(ExpectedException expectedExcpetion, int code, Class<? extends MongoException> exception) {
    expectedExcpetion.expect(exception);
    expectedExcpetion.expect(equalCode(code));
    return expectedExcpetion;
  }

  private static Matcher<Throwable> equalCode(final int code) {
    return new TypeSafeMatcher<Throwable>() {
      public void describeTo(Description description) {
        description.appendText("exception must have code " + code);
      }

      @Override
      public boolean matchesSafely(Throwable item) {
        MongoException mongoException = (MongoException) item;
        return mongoException.getCode() == code;
      }
    };
  }

}
