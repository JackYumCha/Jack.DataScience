using System;

namespace Jack.DataScience.Geo.DataTypes
{
    public class GeoXYZ
    {
        public double X;
        public double Y;
        public double Z;
        public GeoLngLat ToLngLat()
        {
            return new GeoLngLat()
            {
                Lat = 180d * Math.Atan2(Z, Math.Sqrt(X * X + Y * Y)) / Math.PI,
                Lng = 180d * Math.Atan2(Y, X) / Math.PI,
            };
        }
        public override string ToString()
        {
            return $"(X:{X.ToString("0.0000000000")},Y:{Y.ToString("0.0000000000")},Z:{Z.ToString("0.0000000000")})";
        }
        public static GeoXYZ operator *(GeoXYZ v, double s) =>
            new GeoXYZ()
            {
                X = v.X * s,
                Y = v.Y * s,
                Z = v.Z * s,
            };
        public static GeoXYZ operator *(double s, GeoXYZ v) =>
            new GeoXYZ()
            {
                X = v.X * s,
                Y = v.Y * s,
                Z = v.Z * s,
            };

        public static GeoXYZ operator +(GeoXYZ v1, GeoXYZ v2) =>
            new GeoXYZ()
            {
                X = v1.X + v2.X,
                Y = v1.Y + v2.Y,
                Z = v1.Z + v2.Z,
            };

        public static GeoXYZ operator -(GeoXYZ v1, GeoXYZ v2) =>
            new GeoXYZ()
            {
                X = v1.X - v2.X,
                Y = v1.Y - v2.Y,
                Z = v1.Z - v2.Z,
            };
    }


}
