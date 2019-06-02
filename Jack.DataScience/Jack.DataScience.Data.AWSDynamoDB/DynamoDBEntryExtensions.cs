using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2.DocumentModel;

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
    }
}
