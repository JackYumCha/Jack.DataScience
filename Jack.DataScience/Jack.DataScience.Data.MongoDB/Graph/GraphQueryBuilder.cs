using MongoDB.Driver;
using System;
using System.Collections.Generic;

namespace Jack.DataScience.Data.MongoDB
{
    public class GraphQueryBuilder<T> : IWithQueryBuilder, IGraphQueryBuilder where T : DocumentBase
    {
        internal MongoContext MongoContext { get; private set; }

        internal GraphQuery<T> Query { get; private set; }

        IWithQuery IWithQueryBuilder.Query => Query;

        IGraphQueryBuilder IWithQueryBuilder.GraphQueryBuilder => this;

        public Type VertexType => typeof(T);

        internal GraphQueryBuilder(
            MongoContext mongoContext,
            Func<FilterDefinitionBuilder<T>, FilterDefinition<T>> vertexFilterExpression = null,
            bool yieldVertex = false
            )
        {
            MongoContext = mongoContext;
            Query = new GraphQuery<T>()
            {
                filter = vertexFilterExpression?.Invoke(Builders<T>.Filter).RenderToBsonDocument(),
                yieldVertex = yieldVertex,
                type = typeof(T).Name,
                withs = new Dictionary<string, EdgeQuery>()
            };
        }

        //public SortedGraph ToGraph()
        //{
        //    var result = MongoContext.InvokeFunction<SortedGraph>("graphQuery", Query);
        //    return result.retval;
        //}

        public SortedGraph ToSortedGraph()
        {
            var result = MongoContext.InvokeFunction<SortedGraph>("sortedGraphQuery", Query);
            return result.retval.Populate();
        }
    }
}
