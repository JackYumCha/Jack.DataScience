function traverse(edgeName, rootId, limit) {
    var results = [];
    var Ids = [rootId];
    var col = db.getCollection(edgeName);
    var travelled = [rootId];
    for (var i = 0; i < limit; i++) {
        var found = col.find({ _from: { $in: Ids } });
        Ids = [];
        found.forEach(function (item) {
            results.push(item);
            if (!travelled.includes(item._to)) {
                Ids.push(item._to);
                travelled.push(item._to);
            }
        });
        if (Ids.length == 0)
            break;
    }
    return results;
}
