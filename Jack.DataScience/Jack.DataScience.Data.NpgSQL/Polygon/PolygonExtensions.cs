using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Geo.DataTypes
{
    

    public static class PolygonExtensions
    {
        const double TenM = 1.569612305760477e-6;
        const double Half = 0.5d;
        const double Sqrt3Over2 = 0.816496580927726d;
        const double Sqrt3 = 1.632993161855452d;

        public static string CreateHexagon(double longitude, double latitude, double scale)
        {
            // R = 6371000 M
            GeoLngLat gll = new GeoLngLat()
            {
                Lng = longitude,
                Lat = latitude
            };

            GeoXYZ h = gll.dHorizontal(), v = gll.dVertical();

            var pos0 = gll.ToXYZ();

            var hexagon = new List<GeoLngLat>
                {
                    (pos0 + scale * TenM * h).ToLngLat(),
                    (pos0 + scale * Half * TenM * h + scale * Sqrt3Over2 * TenM * v).ToLngLat(),
                    (pos0 - scale * Half * TenM * h + scale * Sqrt3Over2 * TenM * v).ToLngLat(),
                    (pos0 - scale * TenM * h).ToLngLat(),
                    (pos0 - scale * Half * TenM * h - scale * Sqrt3Over2 * TenM * v).ToLngLat(),
                    (pos0 + scale * Half * TenM * h - scale * Sqrt3Over2 * TenM * v).ToLngLat()
                };

            StringBuilder stb = new StringBuilder();

            stb.Append("POLYGON ((");
            for (int i = 0; i <= 6; i++)
            {
                int index = i % 6;
                var pos = hexagon[index];

                stb.Append($"{pos.Lng.ToString("0.0000000000")} {pos.Lat.ToString("0.0000000000")}");
                if (i < 6)
                    stb.Append(",");
            }
            stb.Append("))");
            return stb.ToString();
        }
    }
}
