using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    [BsonIgnoreExtraElements]
    public class MongoFunctionResult<T>
    {
        public T retval { get; set; }
        public double ok { get; set; }
    }
}
