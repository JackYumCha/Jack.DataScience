function sortedGraphQuery(query: IGraphQuery): ISortedGraph {
    let entities: {[key: string]: {[id:string]: ICollection}} = {};
    let traces: IGraphTrace[] = [];
    let result: ISortedGraph = {
        entities: entities,
        graph: traces,
    };

    let items = db.getCollection<ICollection>(query.type).find(query.filter);
    items.forEach(item => {
        let node: IGraphTrace = {
            key: null,
            vertexId: item._id,
            vertexType: query.type,
            vertexExists: true,
            depth: 0,
            withs: {},
        };

        // add item to entities dictionary if yield vertex
        if(query.yieldVertex){
            addEntityToSortDictionary(item, query.type, entities);
        }

        for(let key in query.withs){
            let withQuery = query.withs[key];
            withQuery.depth = 1; // root is 0
            node.withs[key] = sortedGraphWith(node, withQuery, entities);
        }
        traces.push(node);
    });
    return result;
}
