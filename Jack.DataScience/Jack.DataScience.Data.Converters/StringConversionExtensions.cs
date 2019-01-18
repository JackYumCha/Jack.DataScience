using System;

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

        public static object As<T>(this string value)
        {
            var type = typeof(T);
            return value.As(type);
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
            throw new Exception("Unexpected");
        }
    }
}
