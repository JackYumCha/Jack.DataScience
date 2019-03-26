using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System;
using System.Reflection;

namespace Jack.DataScience.Data.MongoDB.Serializers
{
    public class TypeBsonSerializer : SerializerBase<Type>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Type value)
        {
            context.Writer.WriteStartDocument();
            context.Writer.WriteName("@type");
            context.Writer.WriteString(value.FullName);
            context.Writer.WriteEndDocument();
        }

        public override Type Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            context.Reader.ReadStartDocument();
            var typeFullname = context.Reader.ReadString();
            var type = Assembly.GetExecutingAssembly().GetType(typeFullname);
            context.Reader.ReadEndDocument();
            return type;
        }
    }
}
