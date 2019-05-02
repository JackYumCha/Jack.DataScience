function findSortedGraphVertex(graphItem: IGraphTrace, edgeQuery: IEdgeQuery, entities: {[key: string]: {[id:string]: ICollection}}): void{
    let vertexCollection = db.getCollection<IEdgeCollection>(graphItem.vertexType);
    let filter = edgeQuery.vertexFilter ? {
        $and: [
            {
                _id: graphItem.vertexId
            },
            edgeQuery.vertexFilter
        ]
    } : {
        _id: graphItem.vertexId
    };
    let vertices = vertexCollection.find(filter);
    if(vertices.hasNext()){
        let vertex = vertices.next();
        graphItem.vertexExists = true;
        addEntityToSortDictionary(vertex, graphItem.vertexType, entities);
    }
}