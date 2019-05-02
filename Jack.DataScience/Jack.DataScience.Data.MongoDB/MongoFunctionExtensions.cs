using Jack.DataScience.Data.MongoDB.Extensions;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public static class MongoFunctionExtensions
    {
        public static List<T> Traverse<T>(this MongoContext mongoContext, string rootId, int limit) where T: EdgeBase
        {
            var result = mongoContext.InvokeFunction<List<T>>("traverse", typeof(T).Name, rootId, limit);
            return result.retval;
        }

        public static TraverseData<TVertex, TEdge> TraverseTrace<TVertex, TEdge>(this MongoContext mongoContext, string rootId, int limit) 
            where TVertex: DocumentBase where TEdge : EdgeBase
        {
            var result = mongoContext.InvokeFunction<TraverseData<TVertex, TEdge>>("traverseTrace", typeof(TEdge).Name, rootId, limit, typeof(TVertex).Name);
            return result.retval;
        }

        public static TreeTraverseResult TraverseTree<TRoot>(this MongoContext mongoContext, string rootId, List<EdgeDefinition> edges, int limit) where TRoot : DocumentBase
        {
            var edgeDefs = edges.Select(e => new TraverseEdgeDefinition()
            {
                direction = e.Direction,
                name = e.Type.Name
            }).ToList();
            var result = mongoContext.InvokeFunction<TreeTraverseResult>("traverseTree", rootId, typeof(TRoot).Name, edgeDefs, limit);
            return result.retval;
        }

        public static string InsertWithRandomId<T>(this MongoContext mongoContext, T document, int lengthOfId, string prefix = "", int retry = 1024) where T : DocumentBase
        {
            var result = mongoContext.InvokeFunction<MongoInsertResult>("insertWithRandomId", typeof(T).Name, document, lengthOfId, prefix, retry);
            document._id = result.retval._id;
            return document._id;
        }

        public static string InsertWithRandomId<T>(this MongoContext mongoContext, T document, Type type, int lengthOfId, string prefix = "", int retry = 1024) where T : DocumentBase
        {
            var result = mongoContext.InvokeFunction<MongoInsertResult>("insertWithRandomId", type.Name, document, lengthOfId, prefix, retry);
            document._id = result.retval._id;
            return document._id;
        }

        public static string InsertWithRandomDigitId<T>(this MongoContext mongoContext, T document, int lengthOfId, string prefix = "", int retry = 1024) where T : DocumentBase
        {
            var result = mongoContext.InvokeFunction<MongoInsertResult>("insertWithRandomDigitId", typeof(T).Name, document, lengthOfId, prefix, retry);
            document._id = result.retval._id;
            return document._id;
        }

        public static string InsertWithRandomDigitId<T>(this MongoContext mongoContext, T document, Type type, int lengthOfId, string prefix = "", int retry = 1024) where T : DocumentBase
        {
            var result = mongoContext.InvokeFunction<MongoInsertResult>("insertWithRandomDigitId", type.Name, document, lengthOfId, prefix, retry);
            document._id = result.retval._id;
            return document._id;
        }

        public static string InsertWithRandomLowerCaseId<T>(this MongoContext mongoContext, T document, int lengthOfId, string prefix = "", int retry = 1024) where T : DocumentBase
        {
            var result = mongoContext.InvokeFunction<MongoInsertResult>("insertWithRandomLowerCaseId", typeof(T).Name, document, lengthOfId, prefix, retry);
            document._id = result.retval._id;
            return document._id;
        }

        public static string InsertWithRandomLowerCaseId<T>(this MongoContext mongoContext, T document, Type type, int lengthOfId, string prefix = "", int retry = 1024) where T : DocumentBase
        {
            var result = mongoContext.InvokeFunction<MongoInsertResult>("insertWithRandomLowerCaseId", type.Name, document, lengthOfId, prefix, retry);
            document._id = result.retval._id;
            return document._id;
        }

        public static GraphQueryBuilder<T> BeginGraph<T>(
            this MongoContext mongoContext,
            Func<FilterDefinitionBuilder<T>, FilterDefinition<T>> vertexFilter = null,
            bool yieldVertex = true
            ) where T: DocumentBase
        {
            return new GraphQueryBuilder<T>(mongoContext, vertexFilter, yieldVertex);
        }
    }
}
