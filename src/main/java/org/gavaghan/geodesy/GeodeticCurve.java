/* Geodesy by Mike Gavaghan
 * 
 * http://www.gavaghan.org/blog/free-source-code/geodesy-library-vincentys-formula/
 * 
 * This code may be freely used and modified on any personal or professional
 * project.  It comes with no warranty.
 */
package org.gavaghan.geodesy;

import java.io.Serializable;

/**
 * This is the outcome of a geodetic calculation. It represents the path and
 * ellipsoidal distance between two GlobalCoordinates for a specified reference
 * ellipsoid.
 * 
 * @author Mike Gavaghan
 */
public class GeodeticCurve implements Serializable
{
   /** Ellipsoidal distance (in meters). */
   private final double mEllipsoidalDistance;

   /** Azimuth (degrees from north). */
   private final double mAzimuth;

   /** Reverse azimuth (degrees from north). */
   private final double mReverseAzimuth;

   /**
    * Create a new GeodeticCurve.
    * @param ellipsoidalDistance ellipsoidal distance in meters
    * @param azimuth azimuth in degrees
    * @param reverseAzimuth reverse azimuth in degrees
    */
   public GeodeticCurve(double ellipsoidalDistance, double azimuth, double reverseAzimuth)
   {
      mEllipsoidalDistance = ellipsoidalDistance;
      mAzimuth = azimuth;
      mReverseAzimuth = reverseAzimuth;
   }

   /**
    * Get the ellipsoidal distance.
    * @return ellipsoidal distance in meters
    */
   public double getEllipsoidalDistance()
   {
      return mEllipsoidalDistance;
   }

   /**
    * Get the azimuth.
    * @return azimuth in degrees
    */
   public double getAzimuth()
   {
      return mAzimuth;
   }

   /**
    * Get the reverse azimuth.
    * @return reverse azimuth in degrees
    */
   public double getReverseAzimuth()
   {
      return mReverseAzimuth;
   }

   /**
    * Get curve as a string.
    * @return
    */
   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();

      buffer.append("s=");
      buffer.append(mEllipsoidalDistance);
      buffer.append(";a12=");
      buffer.append(mAzimuth);
      buffer.append(";a21=");
      buffer.append(mReverseAzimuth);
      buffer.append(";");

      return buffer.toString();
   }
}
