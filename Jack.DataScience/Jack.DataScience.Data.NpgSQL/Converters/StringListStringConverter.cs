using System;
using System.Linq;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text;
using System.Globalization;

namespace Jack.DataScience.DataTypes
{
    public class StringListStringConverter: TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return sourceType == typeof(List<string>);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return TypeMapping.type_string == destinationType;
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            var typeValue = value as List<string>;
            if (typeValue == null)
                return "";
            return string.Join(",", typeValue);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            var typeValue = value as string;
            if (typeValue == null)
                return new List<string>();
            return typeValue
                .Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .ToList();
        }
    }
}
