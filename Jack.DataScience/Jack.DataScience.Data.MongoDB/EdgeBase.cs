using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class EdgeBase: DocumentBase
    {
        [BsonRequired]
        public string _from { get; set; }
        [BsonRequired]
        public string _to { get; set; }
        public string _fromCollection { get; set; }
        public string _toCollection { get; set; }
    }

    public class EdgePresenter<TFrom, TTo, TEdge>
    {
        public TFrom FromOne { get; set; }
        public List<TFrom> FromMany { get; set; }
        public TTo ToOne { get; set; }
        public List<TTo> ToMany { get; set; }
        public List<TEdge> Edges { get; set; }
    }

    public static class EdgeExtensions
    {
        public static Dictionary<string, EdgePresenter<TFrom, TTo, TEdge>> ByFrom<TFrom, TTo, TEdge>(this IEnumerable<EdgePresenter<TFrom, TTo, TEdge>> edges)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            return edges.ToDictionary(edge => edge.FromOne._id, edge => edge);
        }

        public static Dictionary<string, EdgePresenter<TFrom, TTo, TEdge>> ByTo<TFrom, TTo, TEdge>(this IEnumerable<EdgePresenter<TFrom, TTo, TEdge>> edges)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            return edges.ToDictionary(edge => edge.ToOne._id, edge => edge);
        }

        public static string MakeId<TEdge>(this TEdge edge) where TEdge : EdgeBase
        {
            edge._id = $"{edge._fromCollection}.{edge._from}-{edge._toCollection}.{edge._to}";
            return edge._id;
        }
        public static TEdge Link<TFrom, TTo, TEdge>(this IMongoCollection<TEdge> collection, TFrom from, TTo to)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var edge = new TEdge()
            {
                _from = from._id,
                _to = to._id,
                _fromCollection = typeof(TFrom).Name,
                _toCollection = typeof(TTo).Name,
            };
            edge.MakeId();
            collection.UpsertOne(edge);
            return edge;
        }

        public static string MakePathId<TEdge>(this TEdge edge) where TEdge : EdgeBase
        {
            edge._id = $"{edge._from}/{edge._to}";
            return edge._id;
        }

        public static TEdge PathLink<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to) 
            where TFrom : DocumentBase where TTo: DocumentBase where TEdge: EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            var edge = new TEdge()
            {
                _from = from._id,
                _to = to._id,
                _fromCollection = typeof(TFrom).Name,
                _toCollection = typeof(TTo).Name,
            };
            edge.MakePathId();
            collection.UpsertOne(edge);
            return edge;
        }

        public static TEdge PathLinkUniqueTo<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to)
    where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            collection.DeleteMany(e => e._to == to._id && e._toCollection == typeof(TTo).Name);
            var edge = new TEdge()
            {
                _from = from._id,
                _to = to._id,
                _fromCollection = typeof(TFrom).Name,
                _toCollection = typeof(TTo).Name,
            };
            edge.MakePathId();
            collection.UpsertOne(edge);
            return edge;
        }

        public static TEdge PathLinkUniqueFrom<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to)
where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {

            var collection = mongoContext.Collection<TEdge>();
            collection.DeleteMany(e => e._from == from._id && e._fromCollection == typeof(TFrom).Name);
            var edge = new TEdge()
            {
                _from = from._id,
                _to = to._id,
                _fromCollection = typeof(TFrom).Name,
                _toCollection = typeof(TTo).Name,
            };
            edge.MakePathId();
            collection.UpsertOne(edge);
            return edge;
        }

        /// <summary>
        /// link to collection and produce edge id "{from._id}/{to._id}"
        /// </summary>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <typeparam name="TEdge"></typeparam>
        /// <param name="collection"></param>
        /// <param name="edge"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public static TEdge PathLink<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to, TEdge edge)
    where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            edge._from = from._id;
            edge._to = to._id;
            edge._fromCollection = typeof(TFrom).Name;
            edge._toCollection = typeof(TTo).Name;
            edge.MakePathId();
            collection.UpsertOne(edge);
            return edge;
        }

        public static long Unlink<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var result = collection.DeleteMany(Builders<TEdge>.Filter
                .Where(edge => edge._fromCollection == fromCollection &&
                    edge._toCollection == toCollection &&
                    edge._from == from._id &&
                    edge._to == to._id));
            return result.DeletedCount;
        }

        public static long UnlinkFrom<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var result = collection.DeleteMany(Builders<TEdge>.Filter
                .Where(edge => edge._fromCollection == fromCollection &&
                    edge._toCollection == toCollection &&
                    edge._from == from._id));
            return result.DeletedCount;
        }

        public static long UnlinkTo<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to)
    where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var result = collection.DeleteMany(Builders<TEdge>.Filter
                .Where(edge => edge._fromCollection == fromCollection &&
                    edge._toCollection == toCollection &&
                    edge._to == to._id));
            return result.DeletedCount;
        }

        public static long UnlinkAll<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from, TTo to)
    where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var collection = mongoContext.Collection<TEdge>();
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var result = collection.DeleteMany(Builders<TEdge>.Filter
                .Where(edge => edge._fromCollection == fromCollection &&
                    edge._toCollection == toCollection));
            return result.DeletedCount;
        }

        public static EdgePresenter<TFrom, TTo, TEdge> FindEdgesByFrom<TFrom, TTo, TEdge>(this MongoContext mongoContext, TFrom from)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var colEdge = mongoContext.Collection<TEdge>();

            var result = new EdgePresenter<TFrom, TTo, TEdge>()
            {
                FromOne = from,
                FromMany = new List<TFrom>() { from }
            };

            result.Edges = colEdge.AsQueryable()
                .Where(edge => edge._fromCollection == fromCollection
                && edge._toCollection == toCollection
                && edge._from == from._id)
                .ToList();

            var toIds = result.Edges.Select(edge => edge._to).ToList();
            var colTo = mongoContext.Collection<TTo>();

            result.ToMany = colTo.AsQueryable()
                .Where(to => toIds.Contains(to._id))
                .ToList();

            if (result.ToMany.Count == 1) result.ToOne = result.ToMany.First();

            return result;
        }

        /// <summary>
        /// Find all To Edges from a List of From
        /// </summary>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <typeparam name="TEdge"></typeparam>
        /// <param name="mongoContext"></param>
        /// <param name="froms"></param>
        /// <returns></returns>
        public static List<EdgePresenter<TFrom, TTo, TEdge>> FindEdgesByFroms<TFrom, TTo, TEdge>(this MongoContext mongoContext, IEnumerable<TFrom> froms)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var colEdge = mongoContext.Collection<TEdge>();

            var results = froms.Select(from => new EdgePresenter<TFrom, TTo, TEdge>()
            {
                FromOne = from,
                FromMany = new List<TFrom>() { from },
                ToMany = new List<TTo>(),
            }).ToList();

            var resultDict = results.ToDictionary(p => p.FromOne._id, p => p);

            var fromIds = froms.Select(from => from._id).ToList();

            var allEdges = colEdge.AsQueryable()
                .Where(edge => edge._fromCollection == fromCollection
                && edge._toCollection == toCollection
                && fromIds.Contains(edge._from))
                .ToList();

            var from_to = allEdges
                .GroupBy(edge => edge._from)
                .Select(group =>
                {
                    resultDict[group.Key].Edges = group.ToList();
                    return group;
                })
                .ToDictionary(group => group.Key, group => group.ToList());

            var to_from = allEdges
                .GroupBy(edge => edge._to)
                .ToDictionary(group => group.Key, group => group.ToList());

            var toIds = allEdges.Select(edge => edge._to).ToList();
            var colTo = mongoContext.Collection<TTo>();

            var allTos = colTo.AsQueryable()
                .Where(to => toIds.Contains(to._id))
                .ToList();

            allTos.ForEach(
                to => to_from[to._id].ForEach(
                    fromId => resultDict[fromId._from].ToMany.Add(to)
                    )
                );

            results.ForEach(p =>
            {
                if (p.ToMany.Count == 1) p.ToOne = p.ToMany.First();
            });

            return results;
        }

        public static EdgePresenter<TFrom, TTo, TEdge> FindEdgesByTo<TFrom, TTo, TEdge>(this MongoContext mongoContext, TTo to)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var colEdge = mongoContext.Collection<TEdge>();

            var result = new EdgePresenter<TFrom, TTo, TEdge>()
            {
                ToOne = to,
                ToMany = new List<TTo>() { to }
            };

            result.Edges = colEdge.AsQueryable()
                .Where(edge => edge._fromCollection == fromCollection
                && edge._toCollection == toCollection
                && edge._to == to._id)
                .ToList();

            var fromIds = result.Edges.Select(edge => edge._from).ToList();
            var colFrom = mongoContext.Collection<TFrom>();

            result.FromMany = colFrom.AsQueryable()
                .Where(from => fromIds.Contains(from._id))
                .ToList();

            if (result.FromMany.Count == 1) result.FromOne = result.FromMany.First();

            return result;
        }


        /// <summary>
        /// Find all From Edges from a List of To
        /// </summary>
        /// <typeparam name="TFrom"></typeparam>
        /// <typeparam name="TTo"></typeparam>
        /// <typeparam name="TEdge"></typeparam>
        /// <param name="mongoContext"></param>
        /// <param name="tos"></param>
        /// <returns></returns>
        public static List<EdgePresenter<TFrom, TTo, TEdge>> FindEdgesByTos<TFrom, TTo, TEdge>(this MongoContext mongoContext, IEnumerable<TTo> tos)
            where TFrom : DocumentBase where TTo : DocumentBase where TEdge : EdgeBase, new()
        {
            var fromCollection = typeof(TFrom).Name;
            var toCollection = typeof(TTo).Name;
            var colEdge = mongoContext.Collection<TEdge>();

            var results = tos.Select(to => new EdgePresenter<TFrom, TTo, TEdge>()
            {
                ToOne = to,
                ToMany = new List<TTo>() { to },
                FromMany = new List<TFrom>(),
            }).ToList();

            var resultDict = results.ToDictionary(p => p.ToOne._id, p => p);

            var toIds = tos.Select(to => to._id).ToList();

            var allEdges = colEdge.AsQueryable()
                .Where(edge => edge._fromCollection == fromCollection
                && edge._toCollection == toCollection
                && toIds.Contains(edge._to))
                .ToList();

            var to_from = allEdges
                .GroupBy(edge => edge._to)
                .Select(group =>
                {
                    resultDict[group.Key].Edges = group.ToList();
                    return group;
                })
                .ToDictionary(group => group.Key, group => group.ToList());

            var from_to = allEdges
                .GroupBy(edge => edge._from)
                .ToDictionary(group => group.Key, group => group.ToList());

            var fromIds = allEdges.Select(edge => edge._from).ToList();
            var colFrom = mongoContext.Collection<TFrom>();

            var allFroms = colFrom.AsQueryable()
                .Where(from => fromIds.Contains(from._id))
                .ToList();

            allFroms.ForEach(
                from => from_to[from._id].ForEach(
                    fromId => resultDict[fromId._to].FromMany.Add(from)
                    )
                );

            results.ForEach(p =>
            {
                if (p.ToMany.Count == 1) p.ToOne = p.ToMany.First();
            });

            return results;
        }

    }
}
