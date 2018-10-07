using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using GeoJSON.Net.Geometry;

namespace Jack.DataScience.Geo.DataTypes
{
    public struct GeometryPoint
    {
        public GeometryPoint(double x, double y)
        {
            X = x;
            Y = y;
        }

        public double X;
        public double Y;

        public static bool operator == (GeometryPoint p1, GeometryPoint p2) => p1.X == p2.X && p1.Y == p2.Y;
        public static bool operator != (GeometryPoint p1, GeometryPoint p2) => p1.X != p2.X || p1.Y != p2.Y;

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (obj.GetType() != typeof(GeometryPoint)) return false;
            return this == (GeometryPoint)obj;
        }
        public override int GetHashCode()
        {
            List<byte> bytes = new List<byte>();
            bytes.AddRange(BitConverter.GetBytes(X));
            bytes.AddRange(BitConverter.GetBytes(Y));
            return bytes.ToArray().GetHashCode();
        }
    }

    public static class DouglasPeuckerAlgorithm
    {

        public static MultiPolygon ReduceMultiplePolygon(this MultiPolygon multipolygon, double Tolerance)
        {
            List<Polygon> polygons = new List<Polygon>();
            foreach(var polygon in multipolygon.Coordinates)
            {

                List<LineString> lineStrings = new List<LineString>();
                foreach(var ring in polygon.Coordinates)
                {

                    var points = ring.Coordinates.Select(p => new GeometryPoint(p.Longitude, p.Latitude)).ToList();
                    var reduced = points.DouglasPeuckerReduction(Tolerance);
                    if(reduced.Last() != reduced.First())
                    {
                        reduced.Add(reduced.First());
                    }
                    lineStrings.Add(new LineString(reduced.Select(p => new Position(p.Y, p.X))));
                }
                polygons.Add(new Polygon(lineStrings));
            }
            return new MultiPolygon(polygons);
        }

        /// <summary>
        /// Uses the Douglas Peucker algorithim to reduce the number of points.
        /// </summary>
        /// <param name="Points">The points.</param>
        /// <param name="Tolerance">The tolerance.</param>
        /// <returns></returns>
        public static IList<GeometryPoint> DouglasPeuckerReduction(this IList<GeometryPoint> Points, double Tolerance)
        {
            if (Points == null || Points.Count <= 4)
                return Points;

            int firstPoint = 0;
            int lastPoint = Points.Count - 1;
            var pointIndexsToKeep = new List<int>();

            //Add the first and last index to the keepers
            pointIndexsToKeep.Add(firstPoint);
            pointIndexsToKeep.Add(lastPoint);

            //The first and the last point can not be the same
            while (Points[firstPoint].Equals(Points[lastPoint]))
            {
                lastPoint--;
            }

            DouglasPeuckerReduction(Points, firstPoint, lastPoint, Tolerance, ref pointIndexsToKeep);

            var returnPoints = new List<GeometryPoint>();

            var time = DateTime.Now;
            var rand = new Random(time.Millisecond + time.Second * 1000 + time.Minute * 60000 + time.Hour * 3600000);
            List<int> pointIndices = Points.Select((p, i) => i).ToList();

            while(pointIndexsToKeep.Count < 4)
            {
                var exception = pointIndices.Except(pointIndexsToKeep).ToList();
                int index = exception[(int)Math.Floor(rand.NextDouble() * exception.Count)];
                pointIndexsToKeep.Add(index);
            }

            pointIndexsToKeep.Sort();
            foreach (int index in pointIndexsToKeep)
            {
                returnPoints.Add(Points[index]);
            }

            return returnPoints;
        }

        /// <summary>
        /// Douglases the peucker reduction.
        /// </summary>
        /// <param name="points">The points.</param>
        /// <param name="firstPoint">The first point.</param>
        /// <param name="lastPoint">The last point.</param>
        /// <param name="tolerance">The tolerance.</param>
        /// <param name="pointIndexsToKeep">The point indexs to keep.</param>
        private static void DouglasPeuckerReduction(IList<GeometryPoint> points, int firstPoint, int lastPoint, double tolerance, ref List<int> pointIndexsToKeep)
        {
            double maxDistance = 0;
            int indexFarthest = 0;

            for (int index = firstPoint; index < lastPoint; index++)
            {
                double distance = PerpendicularDistance(points[firstPoint], points[lastPoint], points[index]);
                if (distance > maxDistance)
                {
                    maxDistance = distance;
                    indexFarthest = index;
                }
            }

            if (maxDistance > tolerance && indexFarthest != 0)
            {
                //Add the largest point that exceeds the tolerance
                pointIndexsToKeep.Add(indexFarthest);

                DouglasPeuckerReduction(points, firstPoint, indexFarthest, tolerance, ref pointIndexsToKeep);
                DouglasPeuckerReduction(points, indexFarthest, lastPoint, tolerance, ref pointIndexsToKeep);
            }
        }

        public static double PerpendicularDistance(GeometryPoint Point1, GeometryPoint Point2, GeometryPoint Point)
        {
            //Area = |(1/2)(x1y2 + x2y3 + x3y1 - x2y1 - x3y2 - x1y3)|   *Area of triangle
            //Base = √((x1-x2)²+(x1-x2)²)                               *Base of Triangle*
            //Area = .5*Base*H                                          *Solve for height
            //Height = Area/.5/Base

            double area = Math.Abs(.5 * (Point1.X * Point2.Y + Point2.X * Point.Y + Point.X * Point1.Y - Point2.X * Point1.Y - Point.X * Point2.Y - Point1.X * Point.Y));
            double bottom = Math.Sqrt(Math.Pow(Point1.X - Point2.X, 2) + Math.Pow(Point1.Y - Point2.Y, 2));
            double height = area / bottom * 2;

            return height;
        }

    }
}
