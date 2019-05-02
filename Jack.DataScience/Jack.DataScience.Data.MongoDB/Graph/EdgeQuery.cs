using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{

    public class EdgeQuery: IWithQuery
    {
        public string key { get; set; }
        public string edge { get; set; }
        [BsonIgnoreIfNull]
        public string type { get; set; }
        public bool yieldEdge { get; set; }
        public bool yieldVertex { get; set; }
        public Direction direction { get; set; }
        public int times { get; set; } = 1;
        public int vertexMatches { get; set; } = 65535;
        public Dictionary<string, EdgeQuery> withs { get; set; }
    }
    public class EdgeQuery<TEdge, TVertex>: EdgeQuery, IWithQuery
    {
        [BsonIgnoreIfNull]
        public BsonDocument edgeFilter { get; set; }
        [BsonIgnoreIfNull]
        public BsonDocument vertexFilter { get; set; }
    }
}
