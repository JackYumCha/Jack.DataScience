using System;
using System.Reflection;
using System.Collections.Generic;
using System.Text;

namespace System.ComponentModel.DataAnnotations.Schema
{
    [AttributeUsage(AttributeTargets.Property)]
    public class ForceTypeAttribute: Attribute
    {
        private static Dictionary<Type, TypeConverter> converters = new Dictionary<Type, TypeConverter>();

        private Type converterType;
        public ForceTypeAttribute(Type forecedType, Type typeConverter)
        {
            ForcedType = forecedType;
            if (!converters.ContainsKey(typeConverter))
            {
                TypeConverter obj = typeConverter.GetConstructor(new Type[] { }).Invoke(new object[] { }) as TypeConverter;
                converters.Add(typeConverter, obj);
            }
            converterType = typeConverter;
        }
        public Type ForcedType { get; private set; }
        public TypeConverter Converter {
            get
            {
                return converters[converterType];
            }
        }
    }
}
