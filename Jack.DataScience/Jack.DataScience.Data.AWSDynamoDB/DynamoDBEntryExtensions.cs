using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;

namespace Jack.DataScience.Data.AWSDynamoDB
{
    public static class DynamoDBEntryExtensions
    {
        public static Type stringType = typeof(string);
        public static Type intType = typeof(int);
        public static Type boolType = typeof(bool);
        public static Type longType = typeof(long);
        public static Type byteType = typeof(byte);
        public static Type doubleType = typeof(double);
        public static Type floatType = typeof(float);
        public static Type dateTimeType = typeof(DateTime);
        public static Type listOfStringType = typeof(List<string>);
        public static Type listOfByteArrayType = typeof(List<byte[]>);
        public static Type listOfIntType = typeof(List<int>);

        public static object AsType(this DynamoDBEntry entry, Type type)
        {
            if (type == stringType)
            {
                return entry.AsString();
            }
            else if (type == intType)
            {
                return entry.AsInt();
            }
            else if (type == boolType)
            {
                return entry.AsBoolean();
            }
            else if(type == longType)
            {
                return entry.AsLong();
            }
            else if (type == dateTimeType)
            {
                return entry.AsDateTime();
            }
            else if (type == doubleType)
            {
                return entry.AsDouble();
            }
            else if (type == floatType)
            {
                return entry.AsSingle();
            }
            else if (type == listOfStringType)
            {
                return entry.AsListOfString();
            }
            else if (type == listOfByteArrayType)
            {
                return entry.AsListOfString();
            }
            return null;
        }

        public static object AsType(this AttributeValue value, Type type)
        {
            if (type == stringType)
            {
                return value.S;
            }
            else if (type == intType)
            {
                int r = 0;
                int.TryParse(value.S, out r);
                return r;
            }
            else if (type == boolType)
            {
                return value.B;
            }
            else if (type == longType)
            {
                long r = 0;
                long.TryParse(value.S, out r);
                return r;
            }
            else if (type == dateTimeType)
            {
                DateTime r;
                DateTime.TryParse(value.S, out r);
                return r;
            }
            else if (type == doubleType)
            {
                double r = 0;
                double.TryParse(value.S, out r);
                return r;
            }
            else if (type == floatType)
            {
                float r = 0;
                float.TryParse(value.S, out r);
                return r;
            }
            else if (type == listOfStringType)
            {
                return value.SS;
            }
            else if (type == listOfByteArrayType)
            {
                return value.BS.Select(m => m.ToArray()).ToList();
            }
            return null;
        }

        public static AttributeValue AsAttributeValue(this object value)
        {
            var type = value.GetType();

            if (type == stringType)
            {
                return new AttributeValue() { S = value as string };
            }
            else if (type == intType)
            {
                return new AttributeValue() { S = ((int)value).ToString() };
            }
            else if (type == boolType)
            {
                return new AttributeValue() { BOOL = (bool)value };
            }
            else if (type == longType)
            {
                return new AttributeValue() { S = ((long)value).ToString() };
            }
            else if (type == dateTimeType)
            {
                return new AttributeValue() { S = ((DateTime)value).ToString() };
            }
            else if (type == doubleType)
            {
                return new AttributeValue() { S = ((double)value).ToString() };
            }
            else if (type == floatType)
            {
                return new AttributeValue() { S = ((float)value).ToString() };
            }
            else if (type == listOfStringType)
            {
                return new AttributeValue() { SS = value as List<string> };
            }
            else if (type == listOfByteArrayType)
            {
                return new AttributeValue() { BS = value as List<MemoryStream> };
            }
            return null;
        }

        public static DynamoDBEntry AsDBEntry(this object attributeValue, JsonSerializerSettings jsonSerializerSettings)
        {
            var type = attributeValue.GetType();
            if (type == DynamoDBEntryExtensions.boolType)
            {
                return new DynamoDBBool((bool)attributeValue);
            }
            else if (type.IsEnum)
            {
                // enum as string
                var value = Enum.GetName(type, attributeValue);
                return (DynamoDBEntry)value;
            }
            else if (type == typeof(string))
            {
                string value = attributeValue as string;
                return (DynamoDBEntry)value;
            }
            else if (type.IsClass)
            {
                string value = JsonConvert.SerializeObject(attributeValue, jsonSerializerSettings);
                return (DynamoDBEntry)value;
            }
            else
            {
                dynamic value = attributeValue;
                return (DynamoDBEntry)value;
            }
        }

        public static T ParseDocument<T>(this Dictionary<string, AttributeValue> document, JsonSerializerSettings jsonSerializerSettings) where T: class, new()
        {
            var type = typeof(T);
            var properties = type.GetProperties();
            var obj = new T();
            foreach (var property in properties)
            {
                if (property.PropertyType.IsEnum)
                {
                    property.SetValue(obj, Enum.Parse(property.PropertyType, document[property.Name].S));
                }
                else if (property.PropertyType == typeof(string))
                {
                    property.SetValue(obj, document[property.Name].AsType(property.PropertyType));
                }
                else if (property.PropertyType.IsClass)
                {
                    var json = document[property.Name].AsType(typeof(string)) as string;

                    property.SetValue(obj, JsonConvert.DeserializeObject(json, property.PropertyType, jsonSerializerSettings));
                }
                else
                {
                    property.SetValue(obj, document[property.Name].AsType(property.PropertyType));
                }
            }
            return obj;
        }

        public static T ParseDocument<T>(this Document document, JsonSerializerSettings jsonSerializerSettings) where T : class, new()
        {
            var type = typeof(T);
            var properties = type.GetProperties();
            var obj = new T();
            foreach (var property in properties)
            {
                if (property.PropertyType.IsEnum)
                {
                    property.SetValue(obj, Enum.Parse(property.PropertyType, document[property.Name].AsString()));
                }
                else if (property.PropertyType == typeof(string))
                {
                    property.SetValue(obj, document[property.Name].AsType(property.PropertyType));
                }
                else if (property.PropertyType.IsClass)
                {
                    var json = document[property.Name].AsType(typeof(string)) as string;

                    property.SetValue(obj, JsonConvert.DeserializeObject(json, property.PropertyType, jsonSerializerSettings));
                }
                else
                {
                    property.SetValue(obj, document[property.Name].AsType(property.PropertyType));
                }
            }
            return obj;
        }

        public static Document BuildDocument<T>(this T obj, JsonSerializerSettings jsonSerializerSettings) where T: class, new()
        {
            var document = new Document();
            var type = typeof(T);
            var properties = type.GetProperties();
            foreach (var property in properties)
            {
                if (property.PropertyType == DynamoDBEntryExtensions.boolType)
                {
                    var value = new DynamoDBBool((bool)property.GetValue(obj));
                    document[property.Name] = value;
                }
                else if (property.PropertyType.IsEnum)
                {
                    // enum as string
                    var value = Enum.GetName(property.PropertyType, property.GetValue(obj));
                    document[property.Name] = value;
                }
                else if (property.PropertyType == typeof(string))
                {
                    dynamic value = property.GetValue(obj);
                    document[property.Name] = value;
                }
                else if (property.PropertyType.IsClass)
                {
                    string value = JsonConvert.SerializeObject(property.GetValue(obj), jsonSerializerSettings);
                    document[property.Name] = value;
                }
                else
                {
                    dynamic value = property.GetValue(obj);
                    document[property.Name] = value;
                }
            }
            return document;
        }
    }
}
