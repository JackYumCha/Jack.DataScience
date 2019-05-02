function sortedGraphWith(graphItem, edgeQuery, entities) {
    if (edgeQuery.times <= 0 || edgeQuery.vertexMatches <= 0)
        return []; // stop here if no times count left
    var results = [];
    switch (edgeQuery.direction) {
        case -1:
            {
                var froms = findSortedGraphFroms(graphItem, edgeQuery, entities);
                froms.forEach(function (item) {
                    findSortedGraphVertex(item, edgeQuery, entities);
                    results.push(item);
                });
            }
            break;
        case 0:
            {
                var froms = findSortedGraphFroms(graphItem, edgeQuery, entities);
                froms.forEach(function (item) {
                    findSortedGraphVertex(item, edgeQuery, entities);
                    results.push(item);
                });
                var tos = findSortedGraphTos(graphItem, edgeQuery, entities);
                tos.forEach(function (item) {
                    findSortedGraphVertex(item, edgeQuery, entities);
                    results.push(item);
                });
            }
            break;
        case 1:
            {
                var tos = findSortedGraphTos(graphItem, edgeQuery, entities);
                tos.forEach(function (item) {
                    findSortedGraphVertex(item, edgeQuery, entities);
                    results.push(item);
                });
            }
            break;
    }
    // add withs
    results
        .filter(function (result) { return result.vertexExists; })
        .forEach(function (result) {
        for (var key in edgeQuery.withs) {
            var withQuery = edgeQuery.withs[key];
            withQuery.depth = edgeQuery.depth + 1;
            result.withs[key] = sortedGraphWith(result, withQuery, entities);
        }
    });
    // apply further queries to the results if there are with
    if (edgeQuery.times > 1) {
        results
            .forEach(function (result) {
            var _a;
            var nextLevelResults = sortedGraphWith(result, {
                key: edgeQuery.key,
                edge: edgeQuery.edge,
                type: edgeQuery.type,
                edgeFilter: edgeQuery.edgeFilter,
                vertexFilter: edgeQuery.vertexFilter,
                yieldEdge: true,
                yieldVertex: true,
                direction: edgeQuery.direction,
                times: edgeQuery.times - 1,
                vertexMatches: result.vertexExists ? edgeQuery.vertexMatches - 1 : edgeQuery.vertexMatches,
                depth: edgeQuery.depth + 1,
                withs: edgeQuery.withs,
            }, entities);
            if (result.withs[edgeQuery.key]) {
                (_a = result.withs[edgeQuery.key]).push.apply(_a, nextLevelResults);
            }
            else {
                result.withs[edgeQuery.key] = nextLevelResults;
            }
        });
    }
    return results;
}
