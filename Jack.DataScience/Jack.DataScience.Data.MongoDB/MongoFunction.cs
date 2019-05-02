using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class MongoFunction: DocumentBase
    {
        public BsonJavaScript value { get; set; }
    }
}
