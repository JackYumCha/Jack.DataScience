function addEntityToSortDictionary(item, type, entities) {
    if (!entities[type]) {
        entities[type] = {};
    }
    if (!entities[type][item._id]) {
        entities[type][item._id] = item;
    }
}
