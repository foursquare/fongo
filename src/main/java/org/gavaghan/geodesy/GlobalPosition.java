/* Geodesy by Mike Gavaghan
 * 
 * http://www.gavaghan.org/blog/free-source-code/geodesy-library-vincentys-formula/
 * 
 * This code may be freely used and modified on any personal or professional
 * project.  It comes with no warranty.
 */
package org.gavaghan.geodesy;

/**
 * <p>
 * Encapsulates a three dimensional location on a globe (GlobalCoordinates
 * combined with an elevation in meters above a reference ellipsoid).
 * </p>
 * <p>
 * See documentation for GlobalCoordinates for details on how latitude and
 * longitude measurements are canonicalized.
 * </p>
 * 
 * @author Mike Gavaghan
 */
public class GlobalPosition extends GlobalCoordinates
{
   /** Elevation, in meters, above the surface of the ellipsoid. */
   private double mElevation;

   /**
    * Creates a new instance of GlobalPosition.
    * 
    * @param latitude latitude in degrees
    * @param longitude longitude in degrees
    * @param elevation elevation, in meters, above the reference ellipsoid
    */
   public GlobalPosition(double latitude, double longitude, double elevation)
   {
      super(latitude, longitude);
      mElevation = elevation;
   }

   /**
    * Creates a new instance of GlobalPosition.
    * 
    * @param coords coordinates of the position
    * @param elevation elevation, in meters, above the reference ellipsoid
    */
   public GlobalPosition(GlobalCoordinates coords, double elevation)
   {
      this(coords.getLatitude(), coords.getLongitude(), elevation);
   }

   /**
    * Get elevation.
    * 
    * @return elevation about the ellipsoid in meters.
    */
   public double getElevation()
   {
      return mElevation;
   }

   /**
    * Set the elevation.
    * 
    * @param elevation elevation about the ellipsoid in meters.
    */
   public void setElevation(double elevation)
   {
      mElevation = elevation;
   }

   /**
    * Compare this position to another. Western longitudes are less than eastern
    * logitudes. If longitudes are equal, then southern latitudes are less than
    * northern latitudes. If coordinates are equal, lower elevations are less
    * than higher elevations
    * 
    * @param other instance to compare to
    * @return -1, 0, or +1 as per Comparable contract
    */
   public int compareTo(GlobalPosition other)
   {
      int retval = super.compareTo(other);

      if (retval == 0)
      {
         if (mElevation < other.mElevation) retval = -1;
         else if (mElevation > other.mElevation) retval = +1;
      }

      return retval;
   }

   /**
    * Get a hash code for this position.
    * 
    * @return
    */
   @Override
   public int hashCode()
   {
      int hash = super.hashCode();

      if (mElevation != 0) hash *= (int) mElevation;

      return hash;
   }

   /**
    * Compare this position to another object for equality.
    * 
    * @param other
    * @return
    */
   @Override
   public boolean equals(Object obj)
   {
      if (!(obj instanceof GlobalPosition)) return false;

      GlobalPosition other = (GlobalPosition) obj;

      return (mElevation == other.mElevation) && (super.equals(other));
   }

   /**
    * Get position as a string.
    */
   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();

      buffer.append(super.toString());
      buffer.append("elevation=");
      buffer.append(Double.toString(mElevation));
      buffer.append("m");

      return buffer.toString();
   }
}
