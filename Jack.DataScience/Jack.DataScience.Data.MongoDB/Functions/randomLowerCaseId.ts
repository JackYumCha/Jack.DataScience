function randomLowerCaseId(length: number, prefix: string): string{
    let chars = ['a','b','c','d','e','f','g','h','i','j','g','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','0','1','2','3','4','5','6','7','8','9'];
    let result = '';

    while(result.length < length){
        result += chars[Math.floor(Math.random() * chars.length)];
    }

    if(typeof prefix == 'string') result = prefix + result;
    return result;
}