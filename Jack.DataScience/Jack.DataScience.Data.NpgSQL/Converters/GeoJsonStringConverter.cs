using System;
using System.Linq;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using GeoJSON.Net.Geometry;
using GeoJSON.Net;
using Newtonsoft.Json;

namespace Jack.DataScience.DataTypes
{
    public class GeoJsonStringConverter: TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return TypeMapping.type_IGeometryObject.IsAssignableFrom(sourceType);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return TypeMapping.type_string == destinationType;
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            IGeometryObject geometry = value as IGeometryObject;
            return JsonConvert.SerializeObject(geometry);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            string stringValue = value as string;
            if (stringValue == null) return null;
            return JsonConvert.DeserializeObject<GeoJSONObject>(stringValue);
        }
    }
}
