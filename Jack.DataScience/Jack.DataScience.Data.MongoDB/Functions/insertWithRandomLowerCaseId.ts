function insertWithRandomLowerCaseId(collectionName: string, item: ICollection, length: number, prefix: string, retry?: number): IInsertResult{
    let col = db.getCollection(collectionName);
    let ok: boolean = false;
    if(typeof retry != 'number'){
        retry = 1024;
    }
    let times = 0;
    while(!ok){
        item._id = randomLowerCaseId(length, prefix);
        let result = col.insert(item);
        if(result.nInserted == 1){
            ok = true;
        }
        else if(result.writeError && result.writeError.code == 11000){
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