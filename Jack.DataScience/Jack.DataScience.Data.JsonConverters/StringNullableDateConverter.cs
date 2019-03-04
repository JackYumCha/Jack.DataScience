using System;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Jack.DataScience.Data.JsonConverters
{
    public class StringNullableDateConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(DateTime?);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            DateTime value = DateTime.Now;
            if (DateTime.TryParseExact(reader.Value as string, "yyyy-MM-dd", null, DateTimeStyles.None, out value)) return value;
            return null;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if(value is DateTime? && ((DateTime?)value).HasValue)
            {
                var token = JToken.FromObject(((DateTime?)value).Value.ToString("yyyy-MM-dd"));
                token.WriteTo(writer);
            }
            else
            {
                var token = JToken.FromObject(null);
                token.WriteTo(writer);
            }
        }
    }
}
