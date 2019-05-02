function findSortedGraphTos(graphItem, edgeQuery, entities) {
    var edgeCollection = db.getCollection(edgeQuery.edge);
    var results = [];
    var edgeFilter = edgeQuery.type ? {
        _from: graphItem.vertexId,
        _fromCollection: graphItem.vertexType,
        _toCollection: edgeQuery.type
    } : {
        _from: graphItem.vertexId,
        _fromCollection: graphItem.vertexType
    };
    var filter = edgeQuery.edgeFilter ? {
        $and: [
            edgeFilter,
            edgeQuery.edgeFilter
        ]
    } : edgeFilter;
    var edges = edgeCollection.find(filter);
    edges.forEach(function (edge) {
        results.push({
            key: edgeQuery.key,
            edgeId: edge._id,
            edgeType: edgeQuery.edge,
            direction: 1,
            vertexId: edge._to,
            vertexType: edge._toCollection,
            depth: edgeQuery.depth,
            withs: {}
        });
        if (edgeQuery.yieldEdge) {
            addEntityToSortDictionary(edge, edgeQuery.edge, entities);
        }
    });
    return results;
}
