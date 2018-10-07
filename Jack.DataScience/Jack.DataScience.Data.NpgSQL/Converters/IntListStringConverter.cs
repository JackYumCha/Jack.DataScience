using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Globalization;

namespace Jack.DataScience.DataTypes
{
    public class IntListStringConverter: TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return sourceType == typeof(List<int>);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return TypeMapping.type_string == destinationType;
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            var typeValue = value as List<int>;
            if (typeValue == null)
                return "";
            return string.Join(",", typeValue);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            var typeValue = value as string;
            if (typeValue == null)
                return new List<int>();
            return typeValue
                .Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(s =>
                {
                    int z;
                    if (int.TryParse(s, out z))
                    {
                        return new Nullable<int>(z);
                    }
                    else
                    {
                        return new Nullable<int>();
                    }
                })
                .Where(i => i.HasValue)
                .ToList();
        }
    }
}
