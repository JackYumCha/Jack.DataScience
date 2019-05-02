function randomDigitId(length, prefix) {
    var chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
    var result = '';
    while (result.length < length) {
        result += chars[Math.floor(Math.random() * chars.length)];
    }
    if (typeof prefix == 'string')
        result = prefix + result;
    return result;
}
