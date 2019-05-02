function traverseTree(
        rootId: string, 
        rootType: string, 
        edgeCollections: {name: string, direction: number}[],
        limit: number
    ): ITreeTraverseResult{

    let Ids: IVertex[] = [{_id: rootId, collection: rootType, direction: 0}];
    let collections: {[key: string]:IMongoCollection<IEdgeCollection>} = {};
    for(let edgeCollection of edgeCollections){
        collections[edgeCollection.name] = db.getCollection<IEdgeCollection>(edgeCollection.name);
    }
    let travelled: string[] = [rootType + ':=:' + rootId];

    let result: ITreeTraverseResult = {
        branches:[],
        // items: {}
    };

    for(let i = 0; i < limit; i ++){
        let newIds: IVertex[] = [];
        Ids.forEach(id =>{
            let found: IVertex[] = [];
            for(let edgeCollection of edgeCollections){
                switch(edgeCollection.direction){
                    case -1:{
                        let foundFrom = collections[edgeCollection.name].find({_to: id._id, _toCollection: id.collection});
                        foundFrom.forEach(item => found.push({
                            _id: item._from,
                            collection: item._fromCollection,
                            direction: -1,
                        }));
                    }
                    break;
                    case 0:{
                        let foundTo = collections[edgeCollection.name].find({_from: id._id, _fromCollection: id.collection});
                        foundTo.forEach(item => found.push({
                            _id: item._to,
                            collection: item._toCollection,
                            direction: 1
                        }));
                        let foundFrom = collections[edgeCollection.name].find({_to: id._id, _toCollection: id.collection});
                        foundFrom.forEach(item => found.push({
                            _id: item._from,
                            collection: item._fromCollection,
                            direction: -1
                        }));
                    }
                    break;
                    case 1:{
                        let foundTo = collections[edgeCollection.name].find({_from: id._id, _fromCollection: id.collection});
                        foundTo.forEach(item => found.push({
                            _id: item._to,
                            collection: item._toCollection,
                            direction: 1
                        }));
                    }
                    break;
                }
            }
            let branch: IBranch = {
                from: id,
                to: found
            };
            if(branch.to.length > 0) result.branches.push(branch);
            found.forEach(vertex => {
                let key = vertex._id + ':=:'+vertex.collection;
                if(!travelled.includes(key)){
                    newIds.push(vertex);
                    travelled.push(key);
                }
            });
            
        });
        Ids = newIds;
        if(Ids.length == 0) break;
    }
    return result;
}