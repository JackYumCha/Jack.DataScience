using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson.Serialization.Attributes;

namespace Jack.DataScience.Data.MongoDB
{

    public abstract class DocumentBase
    {
        [BsonId]
        public string _id { get; set; }
    }

    public abstract class EdgeBase: DocumentBase
    {

        [BsonRequired]
        public string _from { get; set; }
        [BsonRequired]
        public string _to { get; set; }
    }

    public abstract class CosmosDocumentBase: DocumentBase
    {
        public string Key { get; set; }
    }

    public static class DocumentBaseExtensions
    {
        public static TDocument UseNewGuidId<TDocument>(this TDocument cosmosDocumentBase) where TDocument: DocumentBase
        {
            cosmosDocumentBase._id = Guid.NewGuid().ToString();
            return cosmosDocumentBase;
        }
    }
}
