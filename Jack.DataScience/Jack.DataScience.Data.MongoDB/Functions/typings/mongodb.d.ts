
interface IMongoDB {
    getCollection<T>(name: string): IMongoCollection<T>;
    loadServerScripts();
}

interface IMongoCollection<T>{
    find(filter: any): ICursor<T>;
    insert(item: T): IWriteResult;
}
interface ICursor<T>{
    forEach(action: (item: T) => any);
    map<TResult>(mapper: (item: T)=> TResult);
    count():number;
    next(): T;
    hasNext(): boolean;
}
interface ICollection{
    _id: string;
}
interface IEdgeCollection extends ICollection {
    _from: string,
    _to: string;
    _fromCollection: string;
    _toCollection: string;
}
interface ITraverseData {
    edges: IEdgeCollection[];
    vertices: ICollection[];
}
interface IError {
    code: number;
    errmsg: string;
}
interface IWriteResult{
    nInserted: number;
    writeConcernError: IError;
    writeError: IError;
}
interface IInsertResult{
    _id?: string;
    retry?: number;
    errmsg?: string;
    result?: IWriteResult;
}

/** tree traverse data */
interface ITreeTraverseResult{
    /** the first should be root vertex */
    branches: IBranch[]; 
    // items: {[key: string]: any[]}
}

interface IBranch{
    from: IVertex;
    to: IVertex[];
}
interface IVertex{
    _id: string;
    direction: number,
    collection: string;
}

// sortedGraphQuery 

interface IGraphQuery{
    filter: any;
    type: string;
    yieldVertex: boolean;
    withs: {[key:string]:IEdgeQuery};
}

interface IEdgeQuery{
    key: string,
    edge: string;
    type: string;
    yieldEdge: boolean;
    edgeFilter: any;
    vertexFilter: any;
    yieldVertex: boolean;
    direction: number;
    times: number;
    vertexMatches: number;
    /** the depth of the graph search */
    depth: number;
    withs: {[key:string]:IEdgeQuery};
}

interface ISortedGraph{
    entities: {[key: string]: {[id:string]: ICollection}};
    graph: IGraphTrace[];
}

interface IGraphTrace{
    key: string;
    /** indicate edge is found */
    edgeId?: string;
    edgeType?: string;
    vertexId?: string;
    vertexType?: string;
    /** indicate vertex is found */
    vertexExists?: boolean;

    /** edge entity */
    edge?: IEdgeCollection;
    vertex?: ICollection;

    direction?: number;
    depth: number;
    withs?: {[key:string]:IGraphTrace[]};
}

declare const db: IMongoDB;

declare function print(value: any);