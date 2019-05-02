function findSortedGraphFroms(graphItem: IGraphTrace, edgeQuery: IEdgeQuery, entities: {[key: string]: {[id:string]: ICollection}}): IGraphTrace[]{
    let edgeCollection = db.getCollection<IEdgeCollection>(edgeQuery.edge);
    let results: IGraphTrace[] = [];
    let edgeFilter = edgeQuery.type ? {
        _to: graphItem.vertexId,
        _toCollection: graphItem.vertexType,
        _fromCollection: edgeQuery.type   
    }:{
        _to: graphItem.vertexId,
        _toCollection: graphItem.vertexType,
    };
    let filter = edgeQuery.edgeFilter ? {
        $and: [
            edgeFilter,
            edgeQuery.edgeFilter
        ]
    } : edgeFilter;
    let edges = edgeCollection.find(filter);
    edges.forEach(edge => {
        results.push({
            key: edgeQuery.key,
            edgeId: edge._id,
            edgeType: edgeQuery.edge,
            direction: -1,
            vertexId: edge._from,
            vertexType: edge._fromCollection,
            depth: edgeQuery.depth,
            withs: {}
        });
        if(edgeQuery.yieldEdge){
            addEntityToSortDictionary(edge, edgeQuery.edge, entities);
        }
    });
    return results;
}