function traverse(edgeName: string, rootId: string, limit: number){
    let results: IEdgeCollection[] = [];
    let Ids: string[] = [rootId];
    let col = db.getCollection<IEdgeCollection>(edgeName);
    let travelled: string[] = [rootId];
    for(let i = 0; i < limit; i ++){
        let found = col.find({_from: {$in: Ids}});
        Ids = [];
        found.forEach(item => {
            results.push(item);
            if(!travelled.includes(item._to)) {
                Ids.push(item._to);
                travelled.push(item._to);
            }
        });
        if(Ids.length == 0) break;
    }

    return results;
}