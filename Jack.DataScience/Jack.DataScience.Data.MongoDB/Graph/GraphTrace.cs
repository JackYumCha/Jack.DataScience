using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Jack.DataScience.Data.MongoDB
{
    public class GraphTrace
    {
        public string key { get; set; }
        public string edgeId { get; set; }
        public string edgeType { get; set; }
        public string vertexId { get; set; }
        public string vertexType { get; set; }
        public bool vertexExists { get; set; }
        public int depth { get; set; }
        public BsonDocument edge { get; set; }
        public BsonDocument vertex { get; set; }
        public Direction direction { get; set; }
        public Dictionary<string, List<GraphTrace>> withs { get; set; }
        /// <summary>
        /// this is added after the graph is built in .net
        /// </summary>
        [BsonIgnore]
        public GraphTrace Parent { get; set; }
    }
}
