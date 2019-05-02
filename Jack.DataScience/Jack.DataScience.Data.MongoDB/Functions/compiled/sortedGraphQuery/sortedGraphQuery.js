function sortedGraphQuery(query) {
    var entities = {};
    var traces = [];
    var result = {
        entities: entities,
        graph: traces,
    };
    var items = db.getCollection(query.type).find(query.filter);
    items.forEach(function (item) {
        var node = {
            key: null,
            vertexId: item._id,
            vertexType: query.type,
            vertexExists: true,
            depth: 0,
            withs: {},
        };
        // add item to entities dictionary if yield vertex
        if (query.yieldVertex) {
            addEntityToSortDictionary(item, query.type, entities);
        }
        for (var key in query.withs) {
            var withQuery = query.withs[key];
            withQuery.depth = 1; // root is 0
            node.withs[key] = sortedGraphWith(node, withQuery, entities);
        }
        traces.push(node);
    });
    return result;
}
