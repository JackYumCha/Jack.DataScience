function findSortedGraphTos(graphItem: IGraphTrace, edgeQuery: IEdgeQuery, entities: {[key: string]: {[id:string]: ICollection}}): IGraphTrace[]{
    let edgeCollection = db.getCollection<IEdgeCollection>(edgeQuery.edge);
    let results: IGraphTrace[] = [];
    let edgeFilter = edgeQuery.type ? {
        _from: graphItem.vertexId,
        _fromCollection: graphItem.vertexType,
        _toCollection: edgeQuery.type   
    }:{
        _from: graphItem.vertexId,
        _fromCollection: graphItem.vertexType
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
            direction: 1,
            vertexId: edge._to,
            vertexType: edge._toCollection,
            depth: edgeQuery.depth,
            withs: {}
        });
        if(edgeQuery.yieldEdge){
            addEntityToSortDictionary(edge, edgeQuery.edge, entities);
        }
    });
    return results;
}