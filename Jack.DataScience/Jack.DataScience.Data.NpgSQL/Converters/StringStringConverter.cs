using System;
using System.Linq;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text;
using System.Globalization;

namespace Jack.DataScience.DataTypes
{
    public class StringStringConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return TypeMapping.type_string == sourceType;
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return TypeMapping.type_string == destinationType;
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            return value as string;
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            return value as string;
        }
    }
}
