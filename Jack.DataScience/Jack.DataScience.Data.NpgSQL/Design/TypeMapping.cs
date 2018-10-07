using System;
using System.Reflection;
using GeoJSON.Net.Geometry;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Jack.DataScience.DataTypes
{
    public static class TypeMapping
    {
        public readonly static Type type_uint = typeof(uint);
        public readonly static Type type_ulong = typeof(ulong);
        public readonly static Type type_int = typeof(int);
        public readonly static Type type_long = typeof(long);
        public readonly static Type type_string = typeof(string);
        public readonly static Type type_double = typeof(double);
        public readonly static Type type_bool = typeof(bool);
        public readonly static Type type_decimal = typeof(decimal);
        public readonly static Type type_datetime = typeof(DateTime);
        public readonly static Type type_nullable = typeof(Nullable<>);
        public readonly static Type type_GeoPoint = typeof(Point);
        public readonly static Type type_GeoLineString = typeof(LineString);
        public readonly static Type type_GeoPolygon = typeof(Polygon);
        public readonly static Type type_GeoMultiPolygon = typeof(MultiPolygon);
        public readonly static Type type_GeoCollection = typeof(GeometryCollection);
        public readonly static Type type_IGeometryObject = typeof(IGeometryObject);

        public static Type GetMappingType(this PropertyInfo propertyInfo)
        {
            Type propertyType = propertyInfo.PropertyType;
            var forceType = propertyInfo.GetCustomAttribute<ForceTypeAttribute>();
            if (forceType != null)
            {
                propertyType = forceType.ForcedType;
            }
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == type_nullable)
            {
                propertyType = propertyType.GetGenericArguments()[0];
            }
            return propertyType;
        }

        public static FieldTypeEnum MapFieldType(this PropertyInfo propertyInfo)
        {
            var propertyType = propertyInfo.GetMappingType();
            if (propertyType == type_int)
            {
                var auto = propertyInfo.GetCustomAttribute<DatabaseGeneratedAttribute>();
                if (auto != null && auto.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity)
                {
                    return FieldTypeEnum.Serial;
                }
                return FieldTypeEnum.Integer;
            }
            else if (propertyType == type_long)
            {
                var auto = propertyInfo.GetCustomAttribute<DatabaseGeneratedAttribute>();
                if (auto != null && auto.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity)
                {
                    return FieldTypeEnum.BigSerial;
                }
                return FieldTypeEnum.BigInteger;
            }
            else if (propertyType == type_string)
            {
                var length = propertyInfo.GetCustomAttribute<StringLengthAttribute>();
                if (length != null)
                {
                    return FieldTypeEnum.VarChar;
                }
                return FieldTypeEnum.Text;
            }
            else if (propertyType == type_double)
            {
                return FieldTypeEnum.Double;
            }
            else if (propertyType == type_bool)
            {
                return FieldTypeEnum.Boolean;
            }
            else if (propertyType == type_decimal)
            {
                return FieldTypeEnum.Decimal;
            }
            else if (propertyType == type_datetime)
            {
                return FieldTypeEnum.TimeStamp;
            }
            else if (propertyType == type_GeoPoint)
            {
                return FieldTypeEnum.GeometryPoint;
            }
            else if (propertyType == type_GeoLineString)
            {
                return FieldTypeEnum.GeometryLineString;
            }
            else if (propertyType == type_GeoPolygon)
            {
                return FieldTypeEnum.GeometryPolygon;
            }
            else if (propertyType == type_GeoMultiPolygon)
            {
                return FieldTypeEnum.GeometryMultiPolygon;
            }
            else if (propertyType == type_GeoCollection)
            {
                return FieldTypeEnum.GeometryCollection;
            }
            throw new Exception($"Unexpected Type {propertyType.FullName}");
        }

        public static string TableIndex(this TypeFieldSchema field)
            => field.IsGeoType ? $"USING GIST({field.Name})" : $"({field.Name})";

        public static string FieldDeclaration(this TypeFieldSchema field)
            => $"{field.Name} {field.PostgreSQLType}{(field.IsRequired ? " NOT NULL" : "")}{(field.IsPrimaryKey ? " PRIMARY KEY" : "")}";

    }
}
