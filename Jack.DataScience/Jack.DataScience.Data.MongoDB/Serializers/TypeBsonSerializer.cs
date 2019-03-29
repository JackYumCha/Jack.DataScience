using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;

namespace Jack.DataScience.Data.MongoDB.Serializers
{
    public class TypeBsonSerializer : SerializerBase<Type>
    {
        private static bool IsRegistered = false;
        public static void Register()
        {
            if (!Volatile.Read(ref IsRegistered))
            {
                Volatile.Write(ref IsRegistered, true);
                BsonSerializer.RegisterSerializer(new TypeBsonSerializer());
            }
        }
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Type value)
        {
            if(value == null)
            {
                context.Writer.WriteNull();
            }
            else
            {
                context.Writer.WriteStartDocument();
                context.Writer.WriteName("@type");
                context.Writer.WriteString(value.FullName);
                context.Writer.WriteEndDocument();
            }
        }

        public override Type Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var bsonType = context.Reader.GetCurrentBsonType();
            switch (bsonType)
            {
                case BsonType.Null:
                    context.Reader.ReadNull();
                    break;
                case BsonType.Document:
                    {
                        context.Reader.ReadStartDocument();
                        var typeFullname = context.Reader.ReadString();
                        var type = Assembly.GetExecutingAssembly().GetType(typeFullname);
                        context.Reader.ReadEndDocument();
                        return type;
                    }
            }
            return null;
        }
    }
}
