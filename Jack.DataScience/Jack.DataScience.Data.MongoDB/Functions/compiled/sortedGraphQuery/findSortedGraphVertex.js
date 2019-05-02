function findSortedGraphVertex(graphItem, edgeQuery, entities) {
    var vertexCollection = db.getCollection(graphItem.vertexType);
    var filter = edgeQuery.vertexFilter ? {
        $and: [
            {
                _id: graphItem.vertexId
            },
            edgeQuery.vertexFilter
        ]
    } : {
        _id: graphItem.vertexId
    };
    var vertices = vertexCollection.find(filter);
    if (vertices.hasNext()) {
        var vertex = vertices.next();
        graphItem.vertexExists = true;
        addEntityToSortDictionary(vertex, graphItem.vertexType, entities);
    }
}
