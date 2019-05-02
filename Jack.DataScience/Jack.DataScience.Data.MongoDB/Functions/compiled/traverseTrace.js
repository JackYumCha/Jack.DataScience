function traverseTrace(edgeName, rootId, limit, vertexName) {
    var edges = [];
    var Ids = [rootId];
    var col = db.getCollection(edgeName);
    var travelled = [rootId];
    for (var i = 0; i < limit; i++) {
        var found = col.find({ _from: { $in: Ids } });
        Ids = [];
        found.forEach(function (item) {
            edges.push(item);
            if (!travelled.includes(item._to)) {
                Ids.push(item._to);
                travelled.push(item._to);
            }
        });
        if (Ids.length == 0)
            break;
    }
    var colVertices = db.getCollection(vertexName);
    var verticesFound = colVertices.find({ _id: { $in: travelled } });
    var vertices = [];
    verticesFound.forEach(function (item) { return vertices.push(item); });
    return {
        edges: edges,
        vertices: vertices
    };
}
