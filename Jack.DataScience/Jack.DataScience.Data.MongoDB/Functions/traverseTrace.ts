function traverseTrace(edgeName: string, rootId: string, limit: number, vertexName: string): ITraverseData{
    let edges: IEdgeCollection[] = [];
    let Ids: string[] = [rootId];
    let col = db.getCollection<IEdgeCollection>(edgeName);
    let travelled: string[] = [rootId];
    for(let i = 0; i < limit; i ++){
        let found = col.find({_from: {$in: Ids}});
        Ids = [];
        found.forEach(item => {
            edges.push(item);
            if(!travelled.includes(item._to)) {
                Ids.push(item._to);
                travelled.push(item._to);
            }
        });
        if(Ids.length == 0) break;
    }
    let colVertices = db.getCollection<ICollection>(vertexName);
    let verticesFound = colVertices.find({_id: {$in: travelled}});
    let vertices: ICollection[] = [];
    verticesFound.forEach(item => vertices.push(item));
    return {
        edges: edges,
        vertices: vertices
    };
}