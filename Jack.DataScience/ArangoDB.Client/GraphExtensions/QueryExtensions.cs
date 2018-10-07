using System;
using System.Collections.Generic;
using ArangoDB.Client;
using ArangoDB.Client.Data;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace ArangoDB.Client
{
    /// <summary>
    /// Query Extnentions for GraphObjects
    /// </summary>
    public static class QueryExtensions
    {
        /// <summary>
        /// create a database with name
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="databaseName"></param>
        public static void CreateDatabase(this IArangoDatabase arangoDatabase, string databaseName)
        {
            arangoDatabase.CreateDatabase(databaseName);
        }

        /// <summary>
        /// escape the illegal arango key characters
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string EscapeForArangoKey(this string value)
        {
            return Uri.EscapeUriString(value).Replace(@"\", "%5C").Replace(@"/", "%2F").Replace(@".", "%2E");
        }

        /// <summary>
        /// get the id of a GraphBase object
        /// </summary>
        /// <typeparam name="TGraph"></typeparam>
        /// <param name="graphBase"></param>
        /// <returns></returns>
        public static string GetId<TGraph>(this TGraph graphBase) where TGraph : GraphBase => $"{graphBase.GetType().Name}/{graphBase._key}";

        /// <summary>
        /// create the Id of GraphBase object
        /// </summary>
        /// <typeparam name="TGraph"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string MakeGraphId<TGraph>(this string key) where TGraph : GraphBase => $"{typeof(TGraph).Name}/{key}";

        /// <summary>
        /// ensure the Id is in the proper format of graphId
        /// </summary>
        /// <typeparam name="TGraph"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string EnsureGraphId<TGraph>(this string id) where TGraph: GraphBase
        {
            if (!id.StartsWith($"{typeof(TGraph).Name}/"))
                return $"{typeof(TGraph).Name}/{id}";
            else
                return id;
        }

        /// <summary>
        /// get the key from vertex id
        /// </summary>
        /// <typeparam name="TGraph"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string VertexIDtoKey<TGraph>(this string id) where TGraph: VertexBase
        {
            string prefix = $"{typeof(TGraph).Name}/";
            if (id.StartsWith(prefix))
                return id.Substring(prefix.Length);
            return id;
        }

        /// <summary>
        /// check if _key is null or empty. if so, build key with from and to;
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="edge"></param>
        /// <returns></returns>
        public static string EnsureEdgeKey<TEdge, TFrom, TTo>(this TEdge edge) where TEdge : EdgeBase where TFrom: VertexBase where TTo: VertexBase
        {
            if(edge._key == null || edge._key == "")
            {
                if (edge._from != null && edge._to != null && edge._from != "" && edge._to != "")
                {
                    edge._from = edge._from.EnsureGraphId<TFrom>();
                    edge._to = edge._to.EnsureGraphId<TTo>();
                    edge.BuildIdWithFromTo();
                }
            }
            return edge._key;
        }

        /// <summary>
        /// detect if a type is vertex or edge and create the collection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <returns></returns>
        public static bool CreateCollectionIfNotExist<T>(this IArangoDatabase arangoDatabase) where T : GraphBase
        {
            try
            {
                arangoDatabase.CreateCollection(typeof(T).Name, type: typeof(VertexBase).GetTypeInfo().IsAssignableFrom(typeof(T).GetTypeInfo()) ? CollectionType.Document : CollectionType.Edge);
                return true;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("duplicate name"))
                    return false;
                else
                    throw ex;
            }
        }

        /// <summary>
        /// test if the arango collection has this key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool HasKey<T>(this IArangoDatabase arangoDatabase, object key) where T : GraphBase
        {
            return arangoDatabase.Query<T>()
               .Filter((item) => item._id == key.ToString())
               .Return(item => item._key)
               .ToListAsync()
               .GetAwaiter()
               .GetResult()
               .Count > 0;
        }

        /// <summary>
        /// get the vertext by key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static T GetVertexByKey<T>(this IArangoDatabase arangoDatabase, object key) where T: VertexBase
        {
            return arangoDatabase.Query<T>()
                .Filter(item => item._key == key.ToString())
                .Return(item => item)
                .ToListAsync()
                .GetAwaiter()
                .GetResult()
                .FirstOrDefault();
        }

        /// <summary>
        /// get edge by key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static T GetEdgeByKey<T>(this IArangoDatabase arangoDatabase, object key) where T : EdgeBase
        {
            return arangoDatabase.Query<T>()
                .Filter(item => item._key == key.ToString())
                .Return(item => item)
                .ToListAsync()
                .GetAwaiter()
                .GetResult()
                .FirstOrDefault();
        }

        /// <summary>
        /// insert an element to arango collection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="instance">the instance to insert</param>
        /// <returns></returns>
        public static IDocumentIdentifierResult InsertElement<T>(this IArangoDatabase arangoDatabase, T instance) where T : GraphBase
        {
            return arangoDatabase.Insert<T>(instance);
        }

        /// <summary>
        /// upsert an item to arango collection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="item"></param>
        public static void Upsert<T>(this IArangoDatabase arangoDatabase, T item) where T : VertexBase
        {
            arangoDatabase.Query().Upsert<T>(
                arangoDB_item_for_upsert => new { _key = item._key },
                _ => item,
                (_, old) => item)
                .Execute();
        }

        private static readonly JsonSerializer IgnoreNullJsonSerializer = new JsonSerializer() { NullValueHandling = NullValueHandling.Ignore };

        /// <summary>
        /// upsert an item to arango collection and ignore the null properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="item"></param>
        public static void UpsertIgnoreNull<T>(this IArangoDatabase arangoDatabase, T item) where T : VertexBase
        {
            JObject jObject = JObject.FromObject(item, IgnoreNullJsonSerializer);
            arangoDatabase.Query().Upsert<T>(
                arangoDB_item_for_upsert => new { _key = item._key },
                _ => jObject,
                (_, old) => jObject)
                .Execute();
        }

        /// <summary>
        /// upsert edge to arango collection (T) and ensure the from side is unique
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="item"></param>
        public static void UpsertEdgeUniqueFrom<T>(this IArangoDatabase arangoDatabase, T item) where T : EdgeBase
        {
            arangoDatabase.Query<T>()
                .Filter(item_to_remove => item_to_remove._from == item._from)
                .Remove(item_to_remove => new { _key = item_to_remove._key })
                .Execute();
            arangoDatabase.Query().Upsert<T>(
                arangoDB_item_for_upsert => new { _key = item._key },
                _ => item,
                (_, old) => item)
                .Execute();
        }

        /// <summary>
        /// upsert edge to arango collection (T) and ensure the to side is unique
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="item"></param>
        public static void UpsertEdgeUniqueTo<T>(this IArangoDatabase arangoDatabase, T item) where T : EdgeBase
        {
            arangoDatabase.Query<T>()
                .Filter(item_to_remove => item_to_remove._to == item._to)
                .Remove(item_to_remove => new { _key = item_to_remove._key })
                .Execute();
            arangoDatabase.Query().Upsert<T>(
                arangoDB_item_for_upsert => new { _key = item._key },
                _ => item,
                (_, old) => item)
                .Execute();
        }

        /// <summary>
        /// upsert edge to arango edge collection (T)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="item"></param>
        public static void UpsertEdge<T>(this IArangoDatabase arangoDatabase, T item) where T : EdgeBase
        {
            arangoDatabase.Query().Upsert<T>(
                arangoDB_item_for_upsert => new { _key = item._key },
                _ => item,
                (_, old) => item)
                .Execute();
        }


        /// <summary>
        /// build the Id of edge with the keys of from and to vertices
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <param name="edge"></param>
        /// <returns></returns>
        public static string BuildIdWithFromTo<TEdge>(this TEdge edge) where TEdge : EdgeBase
        {
            edge._key = $"{edge._from.Replace("/", ".")}==-{edge._to.Replace("/", ".")}";
            return edge._key;
        }

        /// <summary>
        /// build edge with the from and to vertices
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="edge"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public static string BuildEdge<TEdge, TFrom, TTo>(this TEdge edge, TFrom from, TTo to) where TEdge : EdgeBase where TFrom : VertexBase where TTo : VertexBase
        {
            edge._from = $"{typeof(TFrom).Name}/{from._key}";
            edge._to = $"{typeof(TTo).Name}/{to._key}";
            return edge.BuildIdWithFromTo();
        }

        /// <summary>
        /// build the edge with fromKey and to vertex
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="edge"></param>
        /// <param name="fromKey"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public static string BuildEdge<TEdge, TFrom, TTo>(this TEdge edge, string fromKey, TTo to) where TEdge : EdgeBase where TFrom : VertexBase where TTo : VertexBase
        {
            edge._from = $"{typeof(TFrom).Name}/{fromKey}";
            edge._to = $"{typeof(TTo).Name}/{to._key}";
            return edge.BuildIdWithFromTo();
        }

        /// <summary>
        /// build the edge with from vertex and toKey
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="edge"></param>
        /// <param name="from"></param>
        /// <param name="toKey"></param>
        /// <returns></returns>
        public static string BuildEdge<TEdge, TFrom, TTo>(this TEdge edge, TFrom from, string toKey) where TEdge : EdgeBase where TFrom : VertexBase where TTo : VertexBase
        {
            edge._from = $"{typeof(TFrom).Name}/{from._key}";
            edge._to = $"{typeof(TTo).Name}/{toKey}";
            return edge.BuildIdWithFromTo();
        }

        /// <summary>
        /// build edge with fromKey and toKey
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="edge"></param>
        /// <param name="fromKey"></param>
        /// <param name="toKey"></param>
        /// <returns></returns>
        public static string BuildEdge<TEdge, TFrom, TTo>(this TEdge edge, string fromKey, string toKey) where TEdge : EdgeBase where TFrom : VertexBase where TTo : VertexBase
        {
            edge._from = $"{typeof(TFrom).Name}/{fromKey}";
            edge._to = $"{typeof(TTo).Name}/{toKey}";
            return edge.BuildIdWithFromTo();
        }

        /// <summary>
        /// upsert edge with the from and to instances
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="setter"></param>
        /// <returns></returns>
        public static TEdge UpsertEdge<TEdge, TFrom, TTo>(this IArangoDatabase arangoDatabase, TFrom from, TTo to, Action<TEdge> setter = null) where TEdge : EdgeBase, new() where TFrom : VertexBase where TTo : VertexBase
        {
            TEdge edge = new TEdge();
            edge._from = $"{typeof(TFrom).Name}/{from._key}";
            edge._to = $"{typeof(TTo).Name}/{to._key}";
            edge.BuildIdWithFromTo();
            setter?.Invoke(edge);
            arangoDatabase.UpsertEdge(edge);
            return edge;
        }

        /// <summary>
        /// upsert edge with the from and to instances
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="setter"></param>
        /// <returns></returns>
        public static TEdge UpsertEdge<TEdge, TFrom, TTo>(this IArangoDatabase arangoDatabase, string keyFrom, string keyTo, Action<TEdge> setter = null) where TEdge : EdgeBase, new() where TFrom : VertexBase where TTo : VertexBase
        {
            TEdge edge = new TEdge();
            edge._from = keyFrom.EnsureGraphId<TFrom>();
            edge._to = keyTo.EnsureGraphId<TTo>();
            edge.BuildIdWithFromTo();
            setter?.Invoke(edge);
            arangoDatabase.UpsertEdge(edge);
            return edge;
        }

        /// <summary>
        /// Upsert an edge and maintain its 
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="setter"></param>
        /// <returns></returns>
        public static TEdge UpsertEdgeUniqueFrom<TEdge, TFrom, TTo>(this IArangoDatabase arangoDatabase, TFrom from, TTo to, Action<TEdge> setter = null) where TEdge : EdgeBase, new() where TFrom : VertexBase where TTo : VertexBase
        {
            TEdge edge = new TEdge();
            edge._from = from.GetId();
            edge._to = to.GetId();
            edge.BuildIdWithFromTo();
            setter?.Invoke(edge);
            arangoDatabase.UpsertEdgeUniqueFrom(edge);
            return edge;
        }

        /// <summary>
        /// upsert edge and ensure to side is unique
        /// </summary>
        /// <typeparam name="TEdge"></typeparam>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="setter"></param>
        /// <returns></returns>
        public static TEdge UpsertEdgeUniqueTo<TEdge, TFrom, TTo>(this IArangoDatabase arangoDatabase, TFrom from, TTo to, Action<TEdge> setter = null) where TEdge : EdgeBase, new() where TFrom : VertexBase where TTo : VertexBase
        {
            TEdge edge = new TEdge();
            edge._from = from.GetId();
            edge._to = to.GetId();
            edge.BuildIdWithFromTo();
            setter?.Invoke(edge);
            arangoDatabase.UpsertEdgeUniqueTo(edge);
            return edge;
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <typeparam name="TTarget"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<TTarget> TraverseFromOriginInBound<TTarget>(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
            where TTarget : VertexBase
        {
            var query = arangoDatabase.Query()
                .Traversal<TTarget, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.InBound()
                .Filter(g => AQL.Split(g.Vertex._id, "/")[0] == typeof(TTarget).Name)
                .Return(g => g.Vertex)
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <typeparam name="TTarget"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<TTarget> TraverseFromOriginOutBound<TTarget>(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
            where TTarget : VertexBase
        {
            var query = arangoDatabase.Query()
                .Traversal<TTarget, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.OutBound()
                .Filter(g => AQL.Split(g.Vertex._id, "/")[0] == typeof(TTarget).Name)
                .Return(g => g.Vertex)
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via any direction of edges
        /// </summary>
        /// <typeparam name="TTarget"></typeparam>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<TTarget> TraverseFromOrigin<TTarget>(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
            where TTarget : VertexBase
        {
            var query = arangoDatabase.Query()
                .Traversal<TTarget, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.AnyDirection()
                .Filter(g => AQL.Split(g.Vertex._id, "/")[0] == typeof(TTarget).Name)
                .Return(g => g.Vertex)
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }


        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<TraversalData<JObject, EdgeBase>> TraverseGraphFromOriginInBound(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.InBound()
                .Return(g => g)
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<TraversalData<JObject, EdgeBase>> TraverseGraphFromOriginOutBound(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.OutBound()
                .Return(g => g)
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via any direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<TraversalData<JObject, EdgeBase>> TraverseGraphFromOrigin(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.AnyDirection()
                .Return(g => g)
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }


        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<EdgeTraceData> TraverseEdgeFromOriginInBound(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.InBound()
                .Return(g => new EdgeTraceData() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<EdgeTraceData> TraverseEdgeFromOriginOutBound(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.OutBound()
                .Return(g => new EdgeTraceData() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }


        /// <summary>
        /// Search from the originId to min and max level via any direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<EdgeTraceData> TraverseEdgeFromOrigin(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, EdgeBase>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.AnyDirection()
                .Return(g => new EdgeTraceData() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<RawEdgeTrace> TraverseRawFromOriginInBound(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, JObject>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.InBound()
                .Return(g => new RawEdgeTrace() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<RawEdgeTrace> TraverseRawFromOriginOutBound(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, JObject>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.OutBound()
                .Return(g => new RawEdgeTrace() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }


        /// <summary>
        /// Search from the originId to min and max level via any direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<RawEdgeTrace> TraverseRawFromOrigin(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges)
        {
            var query = arangoDatabase.Query()
                .Traversal<JObject, JObject>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.AnyDirection()
                .Return(g => new RawEdgeTrace() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<EdgeTrace<TVertex, TEdge>> TraverseEdgeFromOriginInBound<TVertex, TEdge>(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges) where TVertex : VertexBase where TEdge : EdgeBase
        {
            var query = arangoDatabase.Query()
                .Traversal<TVertex, TEdge>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.InBound()
                .Return(g => new EdgeTrace<TVertex, TEdge>() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }
        /// <summary>
        /// Search from the originId to min and max level via the outbound direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<EdgeTrace<TVertex, TEdge>> TraverseEdgeFromOriginOutBound<TVertex, TEdge>(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges) where TVertex : VertexBase where TEdge : EdgeBase
        {
            var query = arangoDatabase.Query()
                .Traversal<TVertex, TEdge>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.OutBound()
                .Return(g => new EdgeTrace<TVertex, TEdge>() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }


        /// <summary>
        /// Search from the originId to min and max level via any direction of edges
        /// </summary>
        /// <param name="arangoDatabase"></param>
        /// <param name="originId"></param>
        /// <param name="minDepth"></param>
        /// <param name="maxDepth"></param>
        /// <param name="edges"></param>
        /// <returns></returns>
        public static IEnumerable<EdgeTrace<TVertex, TEdge>> TraverseEdgeFromOrigin<TVertex, TEdge>(
            this IArangoDatabase arangoDatabase,
            string originId,
            int minDepth,
            int maxDepth,
            params Type[] edges) where TVertex: VertexBase where TEdge: EdgeBase
        {
            var query = arangoDatabase.Query()
                .Traversal<TVertex, TEdge>(originId)
                .Depth(minDepth, maxDepth);
            if (edges != null)
                foreach (var type in edges)
                    query = query.Edge(type.Name);
            return query.AnyDirection()
                .Return(g => new EdgeTrace<TVertex, TEdge>() { Edge = g.Edge, Vertex = g.Vertex })
                .ToListAsync()
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// check if the JObject is graph type TGraph
        /// </summary>
        /// <typeparam name="TGraph"></typeparam>
        /// <param name="jObject"></param>
        /// <returns></returns>
        public static bool IsGraphType<TGraph>(this JObject jObject) where TGraph : GraphBase
        {
            string id = jObject.GetValue("_id").Value<string>();
            int index = id.IndexOf("/");
            if (index < 0)
                return false;
            return id.Substring(0, index) == typeof(TGraph).Name;
        }

        /// <summary>
        /// check if the from vertex type is TVertex
        /// </summary>
        /// <typeparam name="TVertex"></typeparam>
        /// <param name="edgeBase"></param>
        /// <returns></returns>
        public static bool IsFromType<TVertex>(this EdgeBase edgeBase) where TVertex : VertexBase
        {
            if (edgeBase == null || edgeBase._from == null)
                return false;
            int index = edgeBase._from.IndexOf("/");
            if (index < 0)
                return false;
            return edgeBase._from.Substring(0, index) == typeof(TVertex).Name;
        }

        /// <summary>
        /// check if the to vertex type is TVertex
        /// </summary>
        /// <typeparam name="TVertex"></typeparam>
        /// <param name="edgeBase"></param>
        /// <returns></returns>
        public static bool IsToType<TVertex>(this EdgeBase edgeBase) where TVertex : VertexBase
        {
            if (edgeBase == null || edgeBase._to == null)
                return false;
            int index = edgeBase._to.IndexOf("/");
            if (index < 0)
                return false;
            return edgeBase._to.Substring(0, index) == typeof(TVertex).Name;
        }
    }
}
