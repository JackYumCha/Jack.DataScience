using Newtonsoft.Json.Linq;

namespace ArangoDB.Client
{
    /// <summary>
    /// the raw data as JObject
    /// </summary>
    public class RawEdgeTrace
    {
        /// <summary>
        /// the vertex at the traced level
        /// </summary>
        public JObject Vertex { get; set; }
        /// <summary>
        /// the edge at the traced level
        /// </summary>
        public JObject Edge { get; set; }
    }

    /// <summary>
    /// the edge trace data for graph
    /// </summary>
    public class EdgeTraceData
    {
        /// <summary>
        /// the vertex at the traced level
        /// </summary>
        public JObject Vertex { get; set;}
        /// <summary>
        /// the edge at the traced level
        /// </summary>
        public EdgeBase Edge { get; set; }
    }

    /// <summary>
    /// Edge Trace of Genertic Type
    /// </summary>
    /// <typeparam name="TVertex"></typeparam>
    /// <typeparam name="TEdge"></typeparam>
    public class EdgeTrace<TVertex , TEdge> where TVertex: VertexBase where TEdge: EdgeBase
    {
        /// <summary>
        /// the vertex at the traced level
        /// </summary>
        public TVertex Vertex { get; set; }
        /// <summary>
        /// the edge at the traced level 
        /// </summary>
        public TEdge Edge { get; set; }
    }
}
