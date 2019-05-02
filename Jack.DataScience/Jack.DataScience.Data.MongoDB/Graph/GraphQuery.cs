using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;

namespace Jack.DataScience.Data.MongoDB
{
    public class GraphQuery: IWithQuery
    {
        public string type { get; set; }
        public bool yieldVertex { get; set; }
        public Dictionary<string, EdgeQuery> withs { get; set; }
    }

    public class GraphQuery<T>: GraphQuery, IWithQuery where T: DocumentBase
    {
        [BsonIgnoreIfNull]
        public BsonDocument filter { get; set; }
    }

    public static class GraphQueryExtensions
    {
        internal static SortedGraph Populate(this SortedGraph sortedGraph)
        {
            sortedGraph.graph.ForEach(graphTrace => graphTrace.Populate(sortedGraph.entities));
            return sortedGraph;
        }

        internal static GraphTrace Populate(this GraphTrace graphTrace, Dictionary<string, Dictionary<string, BsonDocument>> entities)
        {
            graphTrace.edge = entities.TryGet(graphTrace.edgeType)?.TryGet(graphTrace.edgeId);
            graphTrace.vertex = entities.TryGet(graphTrace.vertexType)?.TryGet(graphTrace.vertexId);
            foreach (var subGraphTraces in graphTrace.withs.Values)
            {
                subGraphTraces.ForEach(subGraphTrace =>
                {
                    subGraphTrace.Parent = graphTrace;
                    subGraphTrace.Populate(entities);
                });
            }
            return graphTrace;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TVertex"></typeparam>
        /// <param name="withQuery"></param>
        /// <param name="direction"></param>
        /// <param name="name"></param>
        /// <param name="times"></param>
        /// <param name="vertexMatches"></param>
        /// <param name="yieldEdge">是否返回关联数据</param>
        /// <param name="yieldVertex">是否返回节点数据</param>
        /// <param name="edgeFilter">搜索的关联需要满足的条件</param>
        /// <param name="vertexFilter">搜索的节点需要满足的条件</param>
        /// <param name="subQuery"></param>
        /// <returns></returns>
        public static IWithQueryBuilder With<TEdge, TVertex>(
            this IWithQueryBuilder withQuery,
            Direction direction,
            string name = null,
            int times = 1,
            int vertexMatches = 65535,
            bool yieldEdge = true,
            bool yieldVertex = true,
            Func<FilterDefinitionBuilder<TEdge>, FilterDefinition<TEdge>> edgeFilter = null,
            Func<FilterDefinitionBuilder<TVertex>, FilterDefinition<TVertex>> vertexFilter = null,
            Action<IWithQueryBuilder> subQuery = null
            )
        {
            if(name == null)
            {
                name = $"{withQuery.VertexType.Name}[{(yieldEdge?"*":"")}{typeof(TEdge).Name}({direction.ToSymbol()}{times})>{(yieldVertex ? "*" : "")}{typeof(TVertex).Name}";
            }
            var edge = new EdgeQueryBuilder<TEdge, TVertex>(withQuery.GraphQueryBuilder, direction, name, times, vertexMatches, yieldEdge, yieldVertex, edgeFilter, vertexFilter);
            withQuery.Query.withs.Add(name, edge.Query);
            subQuery?.Invoke(edge);
            return withQuery;
        }

        public static BsonDocument RenderToBsonDocument<T>(this FilterDefinition<T> filter)
        {
            var serializerRegistry = BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<T>();
            return filter.Render(documentSerializer, serializerRegistry);
        }
    }
}
