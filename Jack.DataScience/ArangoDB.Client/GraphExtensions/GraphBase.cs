using ArangoDB.Client;

namespace ArangoDB.Client
{
    /// <summary>
    /// GraphBase is a base class for graph objects
    /// </summary>
    public class GraphBase
    {
        /// <summary>
        /// Id of graph object
        /// </summary>
        public string _id { get; set; }

        /// <summary>
        /// key of graph object
        /// </summary>
        [DocumentProperty(Identifier = IdentifierType.Key)]
        public string _key { get; set; }
    }
}
