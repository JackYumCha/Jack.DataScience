using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;

namespace Jack.DataScience.Data.MongoDB.Serializers
{
    public class TypeBsonSerializer : SerializerBase<Type>
    {
        private static bool IsRegistered = false;
        public static void Register(params Type[] types)
        {
            if (!Volatile.Read(ref IsRegistered))
            {
                foreach(var type in types)
                {
                    if(!Types.ContainsKey(type.FullName))
                        Types.Add(type.FullName, type);
                }
                Volatile.Write(ref IsRegistered, true);
                BsonSerializer.RegisterSerializer(new TypeBsonSerializer());
            }
        }

        private static Dictionary<string, Type> Types = new Dictionary<string, Type>();

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
                        context.Reader.ReadEndDocument();
                        if (Types.ContainsKey(typeFullname))
                        {
                            return Types[typeFullname];
                        }
                        else
                        {
                            return Type.GetType(typeFullname);
                        }
                    }
            }
            return null;
        }
    }
}
