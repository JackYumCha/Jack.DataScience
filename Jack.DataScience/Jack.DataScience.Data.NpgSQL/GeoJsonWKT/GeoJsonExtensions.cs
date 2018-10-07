using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GeoJSON.Net;
using GeoJSON.Net.Geometry;
using System.Text.RegularExpressions;

namespace Jack.DataScience.DataTypes
{
    public static class GeoJsonExtensions
    {
         

        public static string ptnPoint = @"(\-?\d+\.\d*|\-?\d+|\-?\.\d*)\s+(\-?\d+\.\d*|\-?\d+|\-?\.\d*)";
        static string ptnPointRing = $@"\(\s*(\s*{ptnPoint}\s*\,?)+\s*\)";

        /// <summary>
        /// 
        /// </summary>
        static Regex rgxPointFilter = new Regex(ptnPoint);
        /// <summary>
        /// Group[1] will the pionts (1 2, 3 4)
        /// </summary>
        static Regex rgxPolygonRingFilter = new Regex(ptnPointRing);
        


        //static Regex rgxPoint = new Regex(@"POINT\s*\(\s*(\-?\d+|\-?\d+\.\d*)\s+(\-?\d+|\-?\d+\.\d*)\s*\)");
        //static Regex rgxLineString = new Regex(@"LINESTRING\s*\((\s*(\d+|\d+\.\d*)\s+(\d+|\d+\.\d*)\s*\,?)+\s*\)");
        //static Regex rgxPolygon = new Regex(@"POLYGON\s*\((\s*\(\s*(\s*(\d+|\d+\.\d*)\s+(\d+|\d+\.\d*)\s*\,?)+\s*\)\s*\,?)+\s*\)");

        public static GeoJSONObject toGeoJson(this string wkt)
        {
            var wktSections = wkt.PairwisePatternMatch(new Regex(@"(GeometryCollection|MultiPolygon|MultiPoint|MultiLineString|Polygon|LineString|Point|)\s*\(", RegexOptions.IgnoreCase), new Regex(@"\)"), true);

            if (wktSections.Count == 0)
                throw new Exception($@"No Geometry was Found in the Input WKT.");

            if (wktSections.Count > 1)
                throw new Exception($@"More than 1 Geometry were Found in the Input WKT.");

            return wktSections[0].toGeometry() as GeoJSONObject;
        }

        public static GeometryCollection toGeometryCollection(this string wkt)
        {
            var wktSections = wkt.PairwisePatternMatch(new Regex(@"(GeometryCollection|MultiPolygon|MultiPoint|MultiLineString|Polygon|LineString|Point|)\s*\(", RegexOptions.IgnoreCase), new Regex(@"\)"), true);

            if (wktSections.Count == 0)
                throw new Exception($@"No Geometry was Found in the Input WKT.");

            if (wktSections.Count > 1)
                throw new Exception($@"More than 1 Geometry were Found in the Input WKT.");

            var first = wktSections[0].toGeometry();

            GeometryCollection result = first as GeometryCollection;
            if(result == null)
            {
                result = new GeometryCollection(new List<IGeometryObject>() { first });
            }

            return result;
        }

        private static IGeometryObject toGeometry(this PatternPairMatch wktPattern)
        {
            switch (wktPattern.Left.Groups[1].Value.ToLower())
            {
                case "point":
                    return wktPattern.toPoint();
                case "polygon":
                    return wktPattern.toPolygon();
                case "linestring":
                    return wktPattern.toLineString();
                case "multipolygon":
                    return wktPattern.toMultiPolygon();
                case "multipoint":
                    return wktPattern.toMultiPoint();
                case "multilinestring":
                    return wktPattern.toMultiLineString();
                case "geometrycollection":
                    return wktPattern.toGeometryCollection();
                default:
                    throw new Exception($@"Unable to Parse Geometry Type '{wktPattern.Left.Groups[1].Value}'");
            }
        }

        private static MultiPoint toMultiPoint(this PatternPairMatch match)
        {
            IEnumerable<Point> points;
            if(match.Children.Count > 0)
            {
                points = match.Children.Select(pointMatch => new Point(WktPointToPosition(pointMatch.Content)));
            }
            else
            {
                points = WktPointsToPositions(match.Content).Select(position => new Point(position));
            }
            // only 1 level of points
            return new MultiPoint(points);
        }

        private static Point toPoint(this PatternPairMatch match)
        {
            return new Point(WktPointToPosition(match.Content));
        }

        private static LineString toLineString(this PatternPairMatch match)
        {
            // only 1 level of points
            return new LineString(match.Content.WktPointsToPositions());
        }

        private static MultiLineString toMultiLineString(this PatternPairMatch match)
        {
            // only 1 level of points
            return new MultiLineString(match.Children.Select(lineStringMatch => lineStringMatch.toLineString()));
        }

        private static Polygon toPolygon(this PatternPairMatch match)
        {
            // 2 levels of points
            return new Polygon(match.Children.Select(lineStringMatch => lineStringMatch.toLineString()));
        }

        private static MultiPolygon toMultiPolygon(this PatternPairMatch match)
        {
            // 2 levels of points
            return new MultiPolygon(match.Children.Select(polygonMatch => polygonMatch.toPolygon()));
        }

        private static GeometryCollection toGeometryCollection(this PatternPairMatch match)
        {
            return new GeometryCollection(match.Children.Select(geometry => geometry.toGeometry()));
        }

        private static List<IPosition> WktPointsToPositions(this string wktPoints)
        {
            List<IPosition> positions = new List<IPosition>();
            foreach(Match match in rgxPointFilter.Matches(wktPoints))
            {
                positions.Add(new Position(double.Parse(match.Groups[2].Value), double.Parse(match.Groups[1].Value)));
            }
            return positions;
        }
        
        private static IPosition WktPointToPosition(string wktPoint)
        {
            var matchPoint = rgxPointFilter.Match(wktPoint);
            if (matchPoint.Success)
            {
                return new Position(double.Parse(matchPoint.Groups[2].Value), double.Parse(matchPoint.Groups[1].Value));
            }
            else
            {
                throw new Exception($@"Unable to parse WKT POINT from String '{wktPoint}'");
            }
        }


        public static string toWKT(this IGeometryObject geoJSONObject)
        {
            if (geoJSONObject == null)
                return null;
            switch (geoJSONObject.Type)
            {
                case GeoJSONObjectType.GeometryCollection:
                    {
                        GeometryCollection geometryCollection = geoJSONObject as GeometryCollection;
                        return $"GEOMETRYCOLLECTION({string.Join(",", geometryCollection.Geometries.Select(geometry=> geometry.toWKT()))})";
                    }
                case GeoJSONObjectType.MultiPolygon:
                    {
                        MultiPolygon multiPolygon = geoJSONObject as MultiPolygon;
                        return $"MULTIPOLYGON({string.Join(",",multiPolygon.Coordinates.Select(polygon => $"({string.Join(",", polygon.Coordinates.Select(lineString => $"({(string.Join(",", lineString.Coordinates.Select(p => $"{p.Longitude} {p.Latitude}")))})"))})"))})";
                    }
                case GeoJSONObjectType.Polygon:
                    {
                        Polygon polygon = geoJSONObject as Polygon;
                        return $"POLYGON({string.Join(",", polygon.Coordinates.Select(lineString => $"({(string.Join(",", lineString.Coordinates.Select(p => $"{p.Longitude} {p.Latitude}")))})"))})";
                    }
                case GeoJSONObjectType.MultiLineString:
                    {
                        MultiLineString multiLineString = geoJSONObject as MultiLineString;
                        return $"MULTILINESTRING({string.Join(",", multiLineString.Coordinates.Select(lineString => $"({(string.Join(",",lineString.Coordinates.Select(p=>$"{p.Longitude} {p.Latitude}")))})"))})";
                    }
                case GeoJSONObjectType.LineString:
                    {
                        LineString lineString = geoJSONObject as LineString;
                        return $"LINESTRING({string.Join(",", lineString.Coordinates.Select(p => $"{p.Longitude} {p.Latitude}"))})";
                    }
                case GeoJSONObjectType.MultiPoint:
                    {
                        MultiPoint multiPoint = geoJSONObject as MultiPoint;
                        return $"MULTIPOINT({string.Join(",", multiPoint.Coordinates.Select(p => $"{p.Coordinates.Longitude} {p.Coordinates.Latitude}"))})";
                    }
                case GeoJSONObjectType.Point:
                    {
                        Point point = geoJSONObject as Point;
                        return $"POINT({point.Coordinates.Longitude} {point.Coordinates.Latitude})";
                    }
                default:
                    throw new Exception($"Unexpected Type '{geoJSONObject.Type}' for WKT Conversion!");
            }
        }
    }
}
