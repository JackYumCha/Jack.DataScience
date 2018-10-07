using System;

namespace Jack.DataScience.Geo.DataTypes
{
    public class GeoLngLat
    {
        public double Lat;
        public double Lng;

        public override string ToString()
        {
            return $"(Lng:{Lng.ToString("0.0000000000")},Lat:{Lat.ToString("0.0000000000")})";
        }

        public GeoXYZ ToXYZ()
        {
            return new GeoXYZ()
            {
                Z = Math.Sin(Lat * Math.PI / 180d),
                X = Math.Cos(Lat * Math.PI / 180d) * Math.Cos(Lng * Math.PI / 180d),
                Y = Math.Cos(Lat * Math.PI / 180d) * Math.Sin(Lng * Math.PI / 180d),
            };
        }

        public GeoXYZ dVertical()
        {
            return new GeoXYZ()
            {
                X = Math.Sin(Lat * Math.PI / 180d) * Math.Cos(Lng * Math.PI / 180d),
                Y = Math.Sin(Lat * Math.PI / 180d) * Math.Sin(Lng * Math.PI / 180d),
                Z = Math.Cos(Lat * Math.PI / 180d),
            };
        }
        public GeoXYZ dHorizontal()
        {
            return new GeoXYZ()
            {
                X = Math.Sin(Lng * Math.PI / 180d),
                Y = Math.Cos(Lng * Math.PI / 180d),
                Z = 0d,
            };
        }
    }
}
