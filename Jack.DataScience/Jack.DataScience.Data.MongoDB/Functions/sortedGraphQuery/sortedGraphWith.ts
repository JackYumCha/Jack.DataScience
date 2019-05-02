function sortedGraphWith(graphItem: IGraphTrace, edgeQuery: IEdgeQuery, entities: {[key: string]: {[id:string]: ICollection}}){
    if(edgeQuery.times <= 0 || edgeQuery.vertexMatches <= 0) return []; // stop here if no times count left
    let results: IGraphTrace[] = [];
    switch(edgeQuery.direction){
        case -1:{
            let froms = findSortedGraphFroms(graphItem, edgeQuery, entities);
            froms.forEach(item => {
                findSortedGraphVertex(item, edgeQuery, entities);
                results.push(item);
            });
        }
        break;
        case 0:{
            let froms = findSortedGraphFroms(graphItem, edgeQuery, entities);
            froms.forEach(item => {
                findSortedGraphVertex(item, edgeQuery, entities);
                results.push(item);
            });
            let tos = findSortedGraphTos(graphItem, edgeQuery, entities);
            tos.forEach(item => {
                findSortedGraphVertex(item, edgeQuery, entities);
                results.push(item);
            });
        }
        break;
        case 1:{
            let tos = findSortedGraphTos(graphItem, edgeQuery, entities);
            tos.forEach(item => {
                findSortedGraphVertex(item, edgeQuery, entities);
                results.push(item);
            });
        }
        break;
    }

    // add withs
    results
    .filter(result => result.vertexExists)
    .forEach(result => {
        for(let key in edgeQuery.withs){
            let withQuery = edgeQuery.withs[key];
            withQuery.depth = edgeQuery.depth + 1;
            result.withs[key] = sortedGraphWith(result, withQuery, entities);
        }
    });

    // apply further queries to the results if there are with

    if(edgeQuery.times > 1){
        results
        .forEach(result => {
            let nextLevelResults = sortedGraphWith(result, {
                key: edgeQuery.key,
                edge: edgeQuery.edge,
                type: edgeQuery.type,
                edgeFilter: edgeQuery.edgeFilter,
                vertexFilter: edgeQuery.vertexFilter,
                yieldEdge: true,
                yieldVertex: true,
                direction: edgeQuery.direction,
                times: edgeQuery.times - 1,
                vertexMatches: result.vertexExists ? edgeQuery.vertexMatches -1 : edgeQuery.vertexMatches,
                depth: edgeQuery.depth + 1,
                withs: edgeQuery.withs,
            }, entities);
            if(result.withs[edgeQuery.key]){
                result.withs[edgeQuery.key].push(...nextLevelResults);
            }
            else{
                result.withs[edgeQuery.key] = nextLevelResults;
            }
        });
    }

    return results;
}