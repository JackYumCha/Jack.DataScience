using System;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Data.Converters
{
    public static class StringConversionExtensions
    {
        private static Type stringType = typeof(string);
        private static Type intType = typeof(int);
        private static Type intNullableType = typeof(int?);
        private static Type doubleType = typeof(double);
        private static Type doubleNullableType = typeof(double?);
        private static Type longType = typeof(long);
        private static Type longNullableType = typeof(long?);
        private static Type datetimeType = typeof(DateTime);
        private static Type datetimeNullableType = typeof(DateTime?);
        private static Type boolType = typeof(bool);
        private static Type boolNullableType = typeof(bool?);
        private static Type shortType = typeof(short);
        private static Type shortNullableType = typeof(short?);
        private static Type byteType = typeof(byte);
        private static Type byteNullableType = typeof(byte?);

        private static Type stringListType = typeof(List<string>);
        private static Type intListType = typeof(List<int>);
        private static Type longListType = typeof(List<long>);
        private static Type doubleListType = typeof(List<double>);
        private static Type shortListType = typeof(List<short>);
        private static Type boolListType = typeof(List<bool>);
        private static Type byteListType = typeof(List<byte>);

        private static Regex arrayBegin = new Regex(@"^\s*\[");
        private static Regex arrayEnd = new Regex(@"\]\s*$");
        private static Regex arraySplitter = new Regex(@",\s*");
       

        public static T As<T>(this string value)
        {
            var type = typeof(T);
            return (T)value.As(type);
        }

        public static object As(this string value, Type type)
        {
            if(type == stringType)
            {
                return value;
            }
            else if (type == intType)
            {
                int result = 0;
                int.TryParse(value, out result);
                return result;
            }
            else if (type == intNullableType)
            {
                int result = 0;
                if (int.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == doubleType)
            {
                double result = 0;
                double.TryParse(value, out result);
                return result;
            }
            else if (type == doubleNullableType)
            {
                double result = 0;
                if (double.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == longType)
            {
                long result = 0;
                long.TryParse(value, out result);
                return result;
            }
            else if (type == longNullableType)
            {
                long result = 0;
                if (long.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == boolType)
            {
                bool result = false;
                bool.TryParse(value, out result);
                return result;
            }
            else if (type == boolNullableType)
            {
                bool result = false;
                if (bool.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == datetimeType)
            {
                DateTime result = DateTime.UtcNow;
                DateTime.TryParse(value, out result);
                return result;
            }
            else if (type == datetimeNullableType)
            {
                DateTime result = DateTime.UtcNow;
                if (DateTime.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == shortType)
            {
                short result = 0;
                short.TryParse(value, out result);
                return result;
            }
            else if (type == shortNullableType)
            {
                short result = 0;
                if (short.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == byteType)
            {
                byte result = 0;
                byte.TryParse(value, out result);
                return result;
            }
            else if (type == byteNullableType)
            {
                byte result = 0;
                if (byte.TryParse(value, out result)) return result;
                return null;
            }
            else if (type == stringListType)
            {
                // this is not working !!!
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<string>()).ToList();
            }
            else if (type == intListType)
            {
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<int>()).ToList();
            }
            else if(type == doubleListType)
            {
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<double>()).ToList();
            }
            else if(type == longListType)
            {
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<long>()).ToList();
            }
            else if(type == shortListType)
            {
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<short>()).ToList();
            }
            else if(type == byteListType)
            {
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<byte>()).ToList();
            }
            else if(type == boolListType)
            {
                var arrValues = arraySplitter.Split(arrayEnd.Replace(arrayBegin.Replace(value, ""), ""));
                return arrValues.Select(arrValue => arrValue.As<bool>()).ToList();
            }
            throw new Exception("Unexpected");
        }
    }
}
