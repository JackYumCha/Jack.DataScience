using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class SortedGraph
    {
        public Dictionary<string, Dictionary<string, BsonDocument>> entities { get; set; }
        public List<GraphTrace> graph { get; set; }
        //public List<string> logs { get; set; }
    }

    public static class SortedGraphExtensions
    {
        private readonly static Type ListGenericType = typeof(List<>);
        public static Dictionary<string, T> ByType<T>(this SortedGraph sortedGraph) where T: DocumentBase
        {
            var type = typeof(T);
            var dict = sortedGraph.entities.ContainsKey(type.Name) ? sortedGraph.entities[type.Name] : null;
            if (dict == null) return new Dictionary<string, T>();
            return dict.ToDictionary(pair => pair.Key, pair => BsonSerializer.Deserialize<T>(pair.Value));
        }

        public static bool HasTypeId<T>(this SortedGraph sortedGraph, string id) where T : DocumentBase
        {
            var type = typeof(T);
            var dict = sortedGraph.entities.ContainsKey(type.Name) ? sortedGraph.entities[type.Name] : null;
            if (dict == null) return false;
            return dict.ContainsKey(id);
        }

        public static T ByTypeId<T>(this SortedGraph sortedGraph, string id) where T : DocumentBase
        {
            var type = typeof(T);
            var dict = sortedGraph.entities.ContainsKey(type.Name) ? sortedGraph.entities[type.Name] : null;
            if (dict == null) return null;
            return dict.ContainsKey(id)? BsonSerializer.Deserialize<T>(dict[id]) : null;
        }

        public static IEnumerable<T> ByKey<T>(this SortedGraph sortedGraph, string key)
        {
            var type = typeof(T);
            var dict = sortedGraph.entities.ContainsKey(type.Name) ? sortedGraph.entities[type.Name] : null;
            if (dict == null) yield break;
            foreach(var id in sortedGraph.graph.SelectMany(trace => trace.SearchBranchByKey(key)))
            {
                var doc = dict?[id];
                if(doc != null) yield return BsonSerializer.Deserialize<T>(doc);
            }
        }

        public static IEnumerable<BsonDocument> ByKey(this SortedGraph sortedGraph, string key, Type type)
        {
            var dict = sortedGraph.entities.ContainsKey(type.Name)? sortedGraph.entities[type.Name]: null;
            if (dict == null) yield break;
            foreach (var id in sortedGraph.graph.SelectMany(trace => trace.SearchBranchByKey(key)))
            {
                var doc = dict?[id];
                if (doc != null) yield return doc;
            }
        }

        /// <summary>
        /// expand the graph to paths
        /// </summary>
        /// <param name="graphTrace"></param>
        /// <returns></returns>
        public static IEnumerable<IEnumerable<GraphTrace>> Paths(
            this GraphTrace graphTrace,
            string finalKey = null, 
            List<string> allowedKeys = null)
        {
            if(graphTrace.vertexExists && graphTrace.withs.Values.Sum(list => list.Count(gt => gt.vertexExists)) == 0)
            {
                if(finalKey == null || graphTrace.key == finalKey)
                {
                    yield return new List<GraphTrace> { };
                }
            }
            else
            {
                foreach (var pair in graphTrace.withs)
                {
                    if(allowedKeys == null || allowedKeys.Contains(pair.Key))
                    {
                        foreach (var subTrace in pair.Value)
                        {
                            foreach (var subPath in subTrace.Paths(finalKey, allowedKeys))
                            {
                                var path = new List<GraphTrace>() { subTrace };
                                path.AddRange(subPath);
                                yield return path;
                            }
                        }
                    }
                }
            }
        }

        public static IEnumerable<IEnumerable<GraphTrace>> Paths(
            this SortedGraph sortedGraph,
            string finalKey = null,
            List<string> allowedKeys = null)
        {
            return sortedGraph.graph.SelectMany(graphTrace =>
            {
                List<List<GraphTrace>> results = new List<List<GraphTrace>>();
                foreach(var paths in graphTrace.Paths())
                {
                    var path = new List<GraphTrace>() { graphTrace };
                    path.AddRange(paths);
                    results.Add(path);
                }
                return results;
            });
        }


        public static TMap Map<TMap>(this SortedGraph sortedGraph, TMap map)
        {
            var type = typeof(TMap);
            foreach(var property in type.GetProperties())
            {
                var propertyType = property.PropertyType;
                if(propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == ListGenericType)
                {
                    var queryType = propertyType.GetGenericArguments()[0];
                    var documents = sortedGraph.ByKey(property.Name, queryType).ToList();
                    IList list = property.GetValue(map) as IList;
                    foreach(var document in documents)
                    {
                        list.Add(BsonSerializer.Deserialize(document, queryType));
                    }
                }
                else
                {
                    var queryType = property.PropertyType;
                    var documents = sortedGraph.ByKey(property.Name, queryType).ToList();
                    var first = documents.FirstOrDefault();
                    property.SetValue(map, first);
                }
            }
            return map;
        }

        public static IEnumerable<string> SearchBranchByKey(this GraphTrace graphTrace, string key)
        {
            foreach(var name in graphTrace.withs.Keys)
            {
                foreach (var trace in graphTrace.withs[name])
                {
                    if (name == key && trace.vertexExists)
                    {
                        yield return trace.vertexId;
                    }
                    foreach(var id in trace.SearchBranchByKey(key))
                    {
                        yield return id;
                    }
                }
            }
        }

    }
}
