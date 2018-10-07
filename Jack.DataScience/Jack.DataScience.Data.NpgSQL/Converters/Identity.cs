using System;
using System.Linq;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text;
using System.Globalization;


namespace Jack.DataScience.DataTypes
{
    public class Identity : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return true;
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return true;
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            return value;
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            return value;
        }
    }
}
