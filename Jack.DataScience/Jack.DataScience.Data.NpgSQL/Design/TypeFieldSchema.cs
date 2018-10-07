using System;
using System.Reflection;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.DataTypes
{
    public class TypeFieldSchema
    {
        public string Name { get; internal set; }
        public FieldTypeEnum FieldType { get; internal set; }
        public int? MinLength { get; internal set; }
        public int? MaxLength { get; internal set; }
        public TypeConverter Converter { get; internal set; }
        public bool IsIndex { get; internal set; }
        public bool IsPrimaryKey { get; internal set; }
        public bool IsRequired { get; internal set; }
        public bool IsGeoType { get; internal set; }
        public bool IsNullable { get; set; }
        public Type OriginalPropertyType { get; internal set; }
        public PropertyInfo PropertyInfo { get; internal set; }
        public string PostgreSQLType
        {
            get
            {
                switch (FieldType)
                {
                    case FieldTypeEnum.Integer:
                        return "INTEGER";
                    case FieldTypeEnum.Serial:
                        return "SERIAL";
                    case FieldTypeEnum.BigInteger:
                        return "BIGINT";
                    case FieldTypeEnum.BigSerial:
                        return "BIGSERIAL";
                    case FieldTypeEnum.VarChar:
                        return $"VARCHAR({MaxLength})";
                    case FieldTypeEnum.Text:
                        return "TEXT";
                    case FieldTypeEnum.Double:
                        return "DOUBLE";
                    case FieldTypeEnum.Boolean:
                        return "BOOLEAN";
                    case FieldTypeEnum.Decimal:
                        if (MaxLength.HasValue && MinLength.HasValue)
                            return $"DECIMAL({MinLength},{MaxLength - MinLength})";
                        return "DECIMAL";
                    case FieldTypeEnum.TimeStamp:
                        return "TIMESTAMP";
                    case FieldTypeEnum.GeometryPoint:
                        return "GEOMETRY(POINT,26910)";
                    case FieldTypeEnum.GeometryLineString:
                        return "GEOMETRY(LINESTRING,26910)";
                    case FieldTypeEnum.GeometryPolygon:
                        return "GEOMETRY(POLYGON,26910)";
                    case FieldTypeEnum.GeometryMultiPolygon:
                        return "GEOMETRY(MULTIPOLYGON,26910)";
                    case FieldTypeEnum.GeometryCollection:
                        return "GEOMETRY(GEOMETRYCOLLECTION,26910)";
                }
                throw new Exception($"Unexpected Type {OriginalPropertyType.FullName} for PostgreSQL Type Mapping.");
            }
        }

        public string PostgreSQLIndexExpression
        {
            get
            {
                if (IsGeoType)
                    return $"GIST({Name})";
                return Name;
            }
        }

        public string ParameterName
        {
            get => $"@{Name}";
        }

        /// <summary>
        /// add geo converter for geo types
        /// </summary>
        public string WriteParameterName
        {
            get
            {
                if (IsGeoType)
                {
                    if(FieldType== FieldTypeEnum.GeometryCollection)
                    {
                        return $"ST_ForceCollection(ST_MakeValid(ST_GeomFromText(@{Name},26910)))";
                    }
                    else if(FieldType== FieldTypeEnum.GeometryMultiPolygon)
                    {
                        return $"ST_CollectionExtract(ST_ForceCollection(ST_MakeValid(ST_GeomFromText(@{Name},26910))),3)";
                    }
                    else
                    {
                        return $"ST_GeomFromText(@{Name},26910)";
                    }
                }
                else
                {
                    return $"@{Name}";
                }
            }
        }

        public string ReadExpression
        {
            get => IsGeoType ? $"ST_AsText({Name})" : $"{Name}";
        }
    }
}
