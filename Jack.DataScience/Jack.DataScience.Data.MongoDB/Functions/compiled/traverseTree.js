function traverseTree(rootId, rootType, edgeCollections, limit) {
    var Ids = [{ _id: rootId, collection: rootType, direction: 0 }];
    var collections = {};
    for (var _i = 0, edgeCollections_1 = edgeCollections; _i < edgeCollections_1.length; _i++) {
        var edgeCollection = edgeCollections_1[_i];
        collections[edgeCollection.name] = db.getCollection(edgeCollection.name);
    }
    var travelled = [rootType + ':=:' + rootId];
    var result = {
        branches: [],
    };
    var _loop_1 = function (i) {
        var newIds = [];
        Ids.forEach(function (id) {
            var found = [];
            for (var _i = 0, edgeCollections_2 = edgeCollections; _i < edgeCollections_2.length; _i++) {
                var edgeCollection = edgeCollections_2[_i];
                switch (edgeCollection.direction) {
                    case -1:
                        {
                            var foundFrom = collections[edgeCollection.name].find({ _to: id._id, _toCollection: id.collection });
                            foundFrom.forEach(function (item) { return found.push({
                                _id: item._from,
                                collection: item._fromCollection,
                                direction: -1,
                            }); });
                        }
                        break;
                    case 0:
                        {
                            var foundTo = collections[edgeCollection.name].find({ _from: id._id, _fromCollection: id.collection });
                            foundTo.forEach(function (item) { return found.push({
                                _id: item._to,
                                collection: item._toCollection,
                                direction: 1
                            }); });
                            var foundFrom = collections[edgeCollection.name].find({ _to: id._id, _toCollection: id.collection });
                            foundFrom.forEach(function (item) { return found.push({
                                _id: item._from,
                                collection: item._fromCollection,
                                direction: -1
                            }); });
                        }
                        break;
                    case 1:
                        {
                            var foundTo = collections[edgeCollection.name].find({ _from: id._id, _fromCollection: id.collection });
                            foundTo.forEach(function (item) { return found.push({
                                _id: item._to,
                                collection: item._toCollection,
                                direction: 1
                            }); });
                        }
                        break;
                }
            }
            var branch = {
                from: id,
                to: found
            };
            if (branch.to.length > 0)
                result.branches.push(branch);
            found.forEach(function (vertex) {
                var key = vertex._id + ':=:' + vertex.collection;
                if (!travelled.includes(key)) {
                    newIds.push(vertex);
                    travelled.push(key);
                }
            });
        });
        Ids = newIds;
        if (Ids.length == 0)
            return "break";
    };
    for (var i = 0; i < limit; i++) {
        var state_1 = _loop_1(i);
        if (state_1 === "break")
            break;
    }
    return result;
}
