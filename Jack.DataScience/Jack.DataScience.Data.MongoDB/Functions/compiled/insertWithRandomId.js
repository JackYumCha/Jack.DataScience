function insertWithRandomId(collectionName, item, length, prefix, retry) {
    var col = db.getCollection(collectionName);
    var ok = false;
    if (typeof retry != 'number') {
        retry = 1024;
    }
    var times = 0;
    while (!ok && times < retry) {
        item._id = randomId(length, prefix);
        var result = col.insert(item);
        if (result.nInserted == 1) {
            ok = true;
        }
        else if (result.writeError && result.writeError.code == 11000) {
            // continue
            times += 1;
        }
        else {
            return {
                retry: times,
                errmsg: 'error',
                result: result
            };
        }
    }
    return {
        _id: item._id,
        retry: times
    };
}
