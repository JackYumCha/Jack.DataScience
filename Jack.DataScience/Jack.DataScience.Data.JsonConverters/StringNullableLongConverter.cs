using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Jack.DataScience.Data.JsonConverters
{
    public class StringNullableLongConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(long?);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            long value = 0;
            if (long.TryParse(reader.Value as string, out value)) return value;
            return null;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var token = JToken.FromObject(value?.ToString());
            token.WriteTo(writer);
        }
    }
}
