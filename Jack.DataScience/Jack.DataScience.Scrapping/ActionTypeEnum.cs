using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public enum ActionTypeEnum
    {
        // only works on driver
        GoTo,

        // operations
        Click,
        SendKeys,

        Wait, // {time}, [On|Show|Off], [CSS|XPath|Id|Class], {selector}
        LoopWhen, // {count}, [CSS|XPath|Id|Class], {selector}, [<|<=|=|>=|>], {number}, [?Attr|Inter|Outer], {?attr}
        LoopUntil, // {count}, [CSS|XPath|Id|Class], {selector}, [<|<=|=|>=|>], {number}, [?Attr|Inter|Outer], {?attr}

        // filter selection only
        By, // [CSS|XPath|Id|Class], {selector}, [?Root]

        // operation to filter elements
        SkipTake,
        AttrRegex,
        InnerRegex,
        OuterRegex,
        TextRegex,

        // invoke external operation to process data
        Invoke, // {key}

        // element reference
        Yield, // {key}
        Fetch, // {key}
        /// <summary>
        /// Get attribute value
        /// </summary>
        PutAttr, // {key}, {attr}, {?regex}
        PutInner, // {key}, {?regex}
        PutOuter, // {key}, {?regex}
        CollectAttr, // {key}, {attr}, {separator}, {?regex}
        CollectInner, // {key}, {separator}, {?regex}
        CollectOuter, // {key}, {separator}, {?regex}
        SplitOne, // {key}, {separator}, {target}
        LoopSplitOne, // {key}, {separator}, {target}
        // json data operations

        // Create New Object or Replace the existing one with new JObject
        JsonNew, // {key},
        JsonSet, // {key} {field} [Ref|String|Double|Int] {value}
        JsonPush, // {key} {field} [Ref|String] {value}
        // Append String to Json Field
        JsonAdd, // {key} {field} {value}
        JsonDelete, // {key}
        JsonDeleteWhere, // {key-regex}
        JsonUnset, // {key} {field}
        JsonSave, // [Array|Map] [File|S3|BlobStorage] {path}

        Log,
        LogJson,
    }
}
