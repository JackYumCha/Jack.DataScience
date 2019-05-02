using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class EdgeQueryBuilder<TEdge, TVertex> : IWithQueryBuilder
    {
        internal EdgeQuery<TEdge, TVertex> Query { get; private set; }

        IWithQuery IWithQueryBuilder.Query => Query;

        private readonly IGraphQueryBuilder root;
        public IGraphQueryBuilder GraphQueryBuilder => root;

        public Type VertexType => typeof(TVertex);

        internal EdgeQueryBuilder(
            IGraphQueryBuilder root,
            Direction direction,
            string key,
            int times = 1,
            int vertexMatches = 65535,
            bool yieldEdge = true,
            bool yieldVertex = true,
            Func<FilterDefinitionBuilder<TEdge>, FilterDefinition<TEdge>> edgeFilterExpression = null,
            Func<FilterDefinitionBuilder<TVertex>, FilterDefinition<TVertex>> vertexFilterExpression = null
            )
        {
            this.root = root;
            Query = new EdgeQuery<TEdge, TVertex>()
            {
                key = key,
                edge = typeof(TEdge).Name,
                type = typeof(TVertex).Name,
                edgeFilter = edgeFilterExpression?.Invoke(Builders<TEdge>.Filter).RenderToBsonDocument(),
                vertexFilter = vertexFilterExpression?.Invoke(Builders<TVertex>.Filter).RenderToBsonDocument(),
                direction = direction,
                yieldEdge = yieldEdge,
                yieldVertex = yieldVertex,
                times = times,
                vertexMatches = vertexMatches,
                withs = new Dictionary<string, EdgeQuery>()
            };
        }

        //public SortedGraph ToGraph() => root.ToGraph();
        public SortedGraph ToSortedGraph() => root.ToSortedGraph();
    }
}
