using ArangoDB.Client;

namespace ArangoDB.Client
{
    /// <summary>
    /// the base class for Edge object
    /// </summary>
    public class EdgeBase : GraphBase
    {
        /// <summary>
        /// _id of graph object from
        /// </summary>
        [DocumentProperty(Identifier = IdentifierType.EdgeFrom)]
        public virtual string _from { get; set; }
        /// <summary>
        /// _id of graph object to
        /// </summary>
        [DocumentProperty(Identifier = IdentifierType.EdgeTo)]
        public virtual string _to { get; set; }
    }
}
