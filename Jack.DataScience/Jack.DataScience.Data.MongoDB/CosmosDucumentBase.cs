using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson.Serialization.Attributes;

namespace Jack.DataScience.Data.MongoDB
{
    public abstract class CosmosDucumentBase
    {
        [BsonId]
        public object _id { get; set; }
        public string Id { get; set; }
    }
}
