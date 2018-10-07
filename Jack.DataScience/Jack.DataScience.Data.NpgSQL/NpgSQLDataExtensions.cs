using System;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using Jack.DataScience.Common;
using Jack.DataScience.DataTypes;
using Npgsql;
using Newtonsoft.Json;
using GeoJSON.Net.Geometry;

namespace Jack.DataScience.Data.NpgSQL
{
    public static class NpgSQLDataExtensions
    {

        public static void WriteField<T>(this T entity, TypeSchema schema, string fieldName, NpgsqlDataReader reader, int index) where T : class, new()
        {
            var field = schema.GetField(fieldName);
            field.WriteValue(entity, reader, index);
        }

        public static void WriteValue<T>(this TypeFieldSchema field, T entity, NpgsqlDataReader reader, int index) where T: class, new()
        {
            switch (field.FieldType)
            {
                case FieldTypeEnum.Serial:
                case FieldTypeEnum.Integer:
                    if (field.IsNullable)
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetInt32(index), null));
                        }
                    }
                    else
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, default(int), null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetInt32(index), null));
                        }
                    }
                    break;
                case FieldTypeEnum.BigInteger:
                case FieldTypeEnum.BigSerial:
                    if (field.IsNullable)
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetInt64(index), null));
                        }
                    }
                    else
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, default(long), null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetInt64(index), null));
                        }
                    }
                    break;
                case FieldTypeEnum.VarChar:
                case FieldTypeEnum.Text:
                    if (reader.IsDBNull(index))
                    {
                        field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                    }
                    else
                    {
                        field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetString(index), null));
                    }
                    break;
                case FieldTypeEnum.Double:
                    if (field.IsNullable)
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetDouble(index), null));
                        }
                    }
                    else
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, default(double), null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetDouble(index), null));
                        }
                    }
                    break;
                case FieldTypeEnum.Boolean:
                    if (field.IsNullable)
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetBoolean(index), null));
                        }
                    }
                    else
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, default(bool), null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetBoolean(index), null));
                        }
                    }
                    break;
                case FieldTypeEnum.Decimal:
                    if (field.IsNullable)
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetDecimal(index), null));
                        }
                    }
                    else
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, default(decimal), null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetDecimal(index), null));
                        }
                    }
                    break;
                case FieldTypeEnum.TimeStamp:
                    if (field.IsNullable)
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetDateTime(index), null));
                        }
                    }
                    else
                    {
                        if (reader.IsDBNull(index))
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, default(DateTime), null));
                        }
                        else
                        {
                            field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetDateTime(index), null));
                        }
                    }
                    break;
                case FieldTypeEnum.GeometryPoint:
                case FieldTypeEnum.GeometryLineString:
                case FieldTypeEnum.GeometryMultiLineString:
                case FieldTypeEnum.GeometryPolygon:
                case FieldTypeEnum.GeometryMultiPolygon:
                case FieldTypeEnum.GeometryCollection:
                    if (reader.IsDBNull(index))
                    {
                        field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, null, null));
                    }
                    else
                    {
                        field.PropertyInfo.SetValue(entity, field.Converter.ConvertTo(null, null, reader.GetString(index), null));
                    }
                    break;
            }
        }

        public static object ReadField<T>(this T entity, TypeSchema schema, string fieldName) where T: class, new()
        {
            var field = schema.GetField(fieldName);
            return field.ReadValue(entity);
        }

        public static object ReadValue<T>(this TypeFieldSchema field, T entity) where T: class, new()
        {

            var value = field.Converter.ConvertFrom(field.PropertyInfo.GetValue(entity));
            switch (field.FieldType)
            {
                case FieldTypeEnum.Serial:
                case FieldTypeEnum.Integer:
                    {
                        if (value == null && field.IsRequired)
                            return 0;
                        return value;
                    }
                case FieldTypeEnum.BigInteger:
                case FieldTypeEnum.BigSerial:
                    {

                        if (value == null && field.IsRequired)
                            return 0L;
                        return value;
                    }
                case FieldTypeEnum.VarChar:
                case FieldTypeEnum.Text:
                    {
                        if (value == null && field.IsRequired)
                            return "";
                        // we need to know if original type is string or not
                        //if (field.OriginalPropertyType != TypeMapping.type_string)
                        //{
                        //    // use newtonsoft json
                        //    JsonConvert.SerializeObject(value);
                        //}
                        return value;
                    }
                case FieldTypeEnum.Double:
                    {

                        if (value == null && field.IsRequired)
                            return 0d;
                        return value;
                    }
                case FieldTypeEnum.Boolean:
                    {
                        if (value == null && field.IsRequired)
                            return false;
                        return value;
                    }
                case FieldTypeEnum.Decimal:
                    {
                        if (value == null && field.IsRequired)
                            return decimal.Zero;
                        return value;
                    }
                case FieldTypeEnum.TimeStamp:
                    {
                        if (value == null && field.IsRequired)
                            return DateTime.Now;
                        return value;
                    }
                case FieldTypeEnum.GeometryPoint:
                case FieldTypeEnum.GeometryLineString:
                case FieldTypeEnum.GeometryMultiLineString:
                case FieldTypeEnum.GeometryPolygon:
                case FieldTypeEnum.GeometryMultiPolygon:
                case FieldTypeEnum.GeometryCollection:
                    {
                        return value as string;
                        //if (field.OriginalPropertyType == TypeMapping.type_string)
                        //{
                        //    return value;
                        //}
                        //if(TypeMapping.type_IGeometryObject.IsAssignableFrom(field.OriginalPropertyType))
                        //{
                        //    IGeometryObject geometryObject = value as IGeometryObject;
                        //    return geometryObject.toWKT();
                        //}
                        //throw new Exception($"Unexpeced Type '{field.OriginalPropertyType.FullName}' for Geo WKT!");
                    }
                default:
                    throw new Exception($"Unexpeced Type '{field.OriginalPropertyType.FullName}' for Npgsql Read!");
            }
        }

        internal static string MultipleColumnEscapeForUpdateSetter(this string setter, bool IsMultipleColumn)
        {
            return IsMultipleColumn ? $"({setter})" : setter;
        }
    }
}
