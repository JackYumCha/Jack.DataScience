using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public enum ActionTypeEnum
    {
        Null,
        // only works on driver
        GoTo,

        // operations
        Click,
        TryClick,
        JsClick,
        OnClickFailed,
        OnJsClickFailed,
        ScrollIntoView,
        ScrollTo,
        ScrollBy,
        SendKeys,
        ScreenShot, // [Path(local|s3)]
        JS,

        Wait, // {time}, [On|Show|Off], [CSS|XPath|Id|Class], {selector}
        LoopWhen, // {count}, [CSS|XPath|Id|Class], {selector}, [<|<=|=|>=|>], {number}, [[Attribute]|Inter|Outer] Regex
        LoopUntil, // {count}, [CSS|XPath|Id|Class], {selector}, [<|<=|=|>=|>], {number}, [[Attribute]|Inter|Outer] Regex

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
        Put, // {key}, [Text|Inner|Outer|[Attribute-Name]|=value] {?separator} {?regex} {?skip-match} {?take-match}
        Collect, // {key}, [Text|Inner|Outer|[Attribute-Name]|=value] {?separator} {?regex} 
        SwitchBy, // [CSS|XPath|Id|Class], {selector}, [None|Text|[Attribute-Name]|Inner|Outer] {regex} {condition:1} {condition:2} //<0 >=0 =2
        SplitOne, // {key}, {separator}, {target}
        LoopSplitOne, // {key}, {separator}, {target}

        If, // Execute the first "Then" if parent exists, otherwise execute the second "Then"
        // json data operations

        // Create New Object or Replace the existing one with new JObject
        JsonNew, // {key},
        JsonSet, // {key} {field} [Ref|String|Double|Int|Bool] {value}
        JsonPush, // {key} {field} [Ref|String] {value}
        // Append String to Json Field
        JsonAdd, // {key} {field} {value}
        JsonAs, // {key} {as-key}
        JsonDelete, // {key}
        JsonDeleteWhere, // {key-regex}
        JsonUnset, // {key} {field}
        JsonSave, // [One|Array|Map] {path} {?pattern|json-key}

        Log,
        LogData,
        LogJson,
        LogJsonWhere,
        Break,

        HeartBeat, // {instanceId} {job} {task} {payload} {rebootTimeout} {message} {log}

        // declare a set of scripts as function for invoke
        Function, // {function-name}
        // call a defined function
        Call, // {funcation-name}

        // SQS Integration
        SQSSend, // {url} {json-key} {?aws-key} {?aws-secret}
        SQSDelete, // {url} {handle} {?aws-key} {?aws-secret}
        DynamoDBSet, // {key} {json-key} {?aws-key} {?aws-secret}
        
        // update the ScriptJob to DynamoDB and Create SQS Message
        JobUpdate, // {json-key}
        // create a ScriptJob with null payload
        JobNew, // {json-key} {script} {job} {ttl as int} {shouldSchedule as bool}
    }
}
