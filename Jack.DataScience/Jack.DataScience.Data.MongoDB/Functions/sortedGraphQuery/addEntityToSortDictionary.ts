function addEntityToSortDictionary(item: ICollection, type: string, entities: {[key: string]: {[id:string]: ICollection}}){
    if(!entities[type]){
        entities[type] = {};
    }
    if(!entities[type][item._id]){
        entities[type][item._id] = item;
    }
}