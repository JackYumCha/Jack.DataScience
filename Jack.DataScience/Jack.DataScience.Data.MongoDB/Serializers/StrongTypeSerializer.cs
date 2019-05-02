using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Jack.DataScience.Data.MongoDB.Serializers
{
    public class StrongTypeSerializer
    {
        private static bool IsRegistered = false;
        public static void Register(params Type[] types)
        {
            var genericType = typeof(StrongTypeSerializer<>);
            if (!Volatile.Read(ref IsRegistered))
            {
                foreach (var type in types)
                {
                    if (!Types.ContainsKey(type.Name))
                    {
                        Types.Add(type.Name, type);
                        var serializerType = genericType.MakeGenericType(new Type[] { type });
                        var instance = serializerType.GetConstructor(new Type[] { }).Invoke(new object[] { });
                        BsonSerializer.RegisterSerializer(type, instance as IBsonSerializer);
                    }
                }
                Volatile.Write(ref IsRegistered, true);
                //try
                //{
                //    BsonSerializer.RegisterSerializer(new StrongTypeSerializer());
                //}
                //catch { }
            }
        }

        public static Dictionary<string, Type> Types = new Dictionary<string, Type>();
    }
    public class StrongTypeSerializer<T>: SerializerBase<T> where T: DocumentBase
    {

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, T value)
        {
            if (value == null)
            {
                context.Writer.WriteNull();
            }
            else
            {
                var type = value.GetType();
                value.TypeName = type.Name;
                base.Serialize(context, args, value);
                BsonSerializer.Serialize(context.Writer, type, value);
            }
        }

        public override T Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {

            

            var bsonType = context.Reader.GetCurrentBsonType();
            switch (bsonType)
            {
                case BsonType.Null:
                    context.Reader.ReadNull();
                    break;
                case BsonType.Document:
                    {
                        var document = BsonSerializer.Deserialize<BsonDocument>(context.Reader);
                        var typeName = document.GetValue(nameof(DocumentBase.TypeName)).AsString;
                        if (StrongTypeSerializer.Types.ContainsKey(typeName))
                        {
                            return BsonSerializer.Deserialize(document, StrongTypeSerializer.Types[typeName]) as T;
                        }
                        else
                        {
                            return BsonSerializer.Deserialize<T>(document);
                        }
                    }
            }
            return null;
        }
    }
}
