using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Reflection;

namespace Jack.DataScience.Data.JsonConverters
{
    public class TypeJsonConverter : JsonConverter<Type>
    {
        public override void WriteJson(JsonWriter writer, Type value, JsonSerializer serializer)
        {
            var type = value as Type;
            var jObject = JToken.FromObject(new object()) as JObject;
            jObject.Add("TypeFullName", JToken.FromObject(type.FullName));
            jObject.WriteTo(writer);
        }

        public override Type ReadJson(JsonReader reader, Type objectType, Type existingValue, bool hasExistingValue, JsonSerializer serializer)
        {

            var jObject = JObject.Load(reader);
            var jProperty = jObject.Property("TypeFullName");
            var typeFullname = jProperty.Value.Value<string>();
            var type = Assembly.GetExecutingAssembly().GetType(typeFullname);
            return type;
        }
    }
}
