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
 * <p>
 * Encapsulation of latitude and longitude coordinates on a globe. Negative
 * latitude is southern hemisphere. Negative longitude is western hemisphere.
 * </p>
 * <p>
 * Any angle may be specified for longtiude and latitude, but all angles will be
 * canonicalized such that:
 * </p>
 * 
 * <pre>
 * -90 &lt;= latitude &lt;= +90 - 180 &lt; longitude &lt;= +180
 * </pre>
 * 
 * @author Mike Gavaghan
 */
public class GlobalCoordinates implements Comparable<GlobalCoordinates>, Serializable
{
   /** Latitude in degrees. Negative latitude is southern hemisphere. */
   private double mLatitude;

   /** Longitude in degrees. Negative longitude is western hemisphere. */
   private double mLongitude;

   /**
    * Canonicalize the current latitude and longitude values such that:
    * 
    * <pre>
    * -90 &lt;= latitude &lt;= +90 - 180 &lt; longitude &lt;= +180
    * </pre>
    */
   private void canonicalize()
   {
      mLatitude = (mLatitude + 180) % 360;
      if (mLatitude < 0) mLatitude += 360;
      mLatitude -= 180;

      if (mLatitude > 90)
      {
         mLatitude = 180 - mLatitude;
         mLongitude += 180;
      }
      else if (mLatitude < -90)
      {
         mLatitude = -180 - mLatitude;
         mLongitude += 180;
      }

      mLongitude = ((mLongitude + 180) % 360);
      if (mLongitude <= 0) mLongitude += 360;
      mLongitude -= 180;
   }

   /**
    * Construct a new GlobalCoordinates. Angles will be canonicalized.
    * 
    * @param latitude latitude in degrees
    * @param longitude longitude in degrees
    */
   public GlobalCoordinates(double latitude, double longitude)
   {
      mLatitude = latitude;
      mLongitude = longitude;
      canonicalize();
   }

   /**
    * Get latitude.
    * 
    * @return latitude in degrees
    */
   public double getLatitude()
   {
      return mLatitude;
   }

   /**
    * Set latitude. The latitude value will be canonicalized (which might result
    * in a change to the longitude). Negative latitude is southern hemisphere.
    * 
    * @param latitude in degrees
    */
   public void setLatitude(double latitude)
   {
      mLatitude = latitude;
      canonicalize();
   }

   /**
    * Get longitude.
    * 
    * @return longitude in degrees
    */
   public double getLongitude()
   {
      return mLongitude;
   }

   /**
    * Set longitude. The longitude value will be canonicalized. Negative
    * longitude is western hemisphere.
    * 
    * @param longitude in degrees
    */
   public void setLongitude(double longitude)
   {
      mLongitude = longitude;
      canonicalize();
   }

   /**
    * Compare these coordinates to another set of coordiates. Western longitudes
    * are less than eastern logitudes. If longitudes are equal, then southern
    * latitudes are less than northern latitudes.
    * 
    * @param other instance to compare to
    * @return -1, 0, or +1 as per Comparable contract
    */
   public int compareTo(GlobalCoordinates other)
   {
      int retval;

      if (mLongitude < other.mLongitude) retval = -1;
      else if (mLongitude > other.mLongitude) retval = +1;
      else if (mLatitude < other.mLatitude) retval = -1;
      else if (mLatitude > other.mLatitude) retval = +1;
      else retval = 0;

      return retval;
   }

   /**
    * Get a hash code for these coordinates.
    * 
    * @return
    */
   @Override
   public int hashCode()
   {
      return ((int) (mLongitude * mLatitude * 1000000 + 1021)) * 1000033;
   }

   /**
    * Compare these coordinates to another object for equality.
    * 
    * @param other
    * @return
    */
   @Override
   public boolean equals(Object obj)
   {
      if (!(obj instanceof GlobalCoordinates)) return false;

      GlobalCoordinates other = (GlobalCoordinates) obj;

      return (mLongitude == other.mLongitude) && (mLatitude == other.mLatitude);
   }

   /**
    * Get coordinates as a string.
    */
   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();

      buffer.append(Math.abs(mLatitude));
      buffer.append((mLatitude >= 0) ? 'N' : 'S');
      buffer.append(';');
      buffer.append(Math.abs(mLongitude));
      buffer.append((mLongitude >= 0) ? 'E' : 'W');
      buffer.append(';');

      return buffer.toString();
   }
}
