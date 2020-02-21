using Jack.DataScience.Data.AWSAthena;
using Jack.DataScience.StringCompression;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.AWSAthenaEtl
{


    public static class AthenaSQLPipeExtensions
    {


        private static Regex ControlFlowPattern = new Regex(@"^\s*-{2,}\s*(var\s*\[\s*[\w_]+\s*,\s*-?\d+\s*\]|add\s*\[\s*[\w_]+\s*,\s*-?\d+\s*\]|while\s*\(?|for\s*\(\s*[\w_]+\s*,\s*-?\d+\s*,\s*-?\d+\s*\)\s*\{?|for\s*\(\s*[\w_]+\s*,\s*-?\d+\s*,\s*-?\d+\s*,\s*-?\d+\s*\)\s*\{?|if\s*\(?|elseif\s*\(?|else\s*\{?|switch\s*\(?|case\s*\(\s*`[^`]+`\s*\)\s*\{?|case\s*\(\s*""[^""]+""\s*\)\s*\{?|case\s*\(\s*'[^']+'\s*\)\s*\{?|case\s*\(\s*-?\d+\s*\)\s*\{?|default\s*\{?|\)\s*\{|\}\s*\{|\{|\)|\}|\()");

        private static Regex CommentLinePattern = new Regex(@"^\s*--");
        private static Regex EmptyLinePattern = new Regex(@"^\s*$");

        private static Regex WhileBlockPattern = new Regex(@"^\s*-{2,}\s*while\s*(\(?)", RegexOptions.IgnoreCase);
        private static Regex ForBlock1Pattern = new Regex(@"^\s*-{2,}\s*for\s*\(\s*([\w_]+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\)\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex ForBlock2Pattern = new Regex(@"^\s*-{2,}\s*for\s*\(\s*([\w_]+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\)\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex IfBlockPattern = new Regex(@"^\s*-{2,}\s*if\s*(\(?)", RegexOptions.IgnoreCase);
        private static Regex ElseIfBlockPattern = new Regex(@"^\s*-{2,}\s*elseif\s*(\(?)", RegexOptions.IgnoreCase);
        private static Regex ElseBlockPattern = new Regex(@"^\s*-{2,}\s*else\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex SwitchBlockPattern = new Regex(@"^\s*-{2,}\s*switch\s*(\(?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock1Pattern = new Regex(@"^\s*-{2,}\s*case\s*\(\s*`([^`]+)`\s*\)\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock2Pattern = new Regex(@"^\s*-{2,}\s*case\s*\(\s*'([^']+)'\s*\)\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock3Pattern = new Regex(@"^\s*-{2,}\s*case\s*\(\s*""([^""]+)""\s*\)\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock4Pattern = new Regex(@"^\s*-{2,}\s*case\s*\(\s*(-?\d+)\s*\)\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex DefaultBlockPattern = new Regex(@"^\s*-{2,}\s*default\s*(\{?)", RegexOptions.IgnoreCase);
        private static Regex EndEvaluationBlockPattern = new Regex(@"^\s*-{2,}\s*\)", RegexOptions.IgnoreCase);
        private static Regex EndExecutionBlockPattern = new Regex(@"^\s*-{2,}\s*\}", RegexOptions.IgnoreCase);
        private static Regex EndEvaluationBeginExecutionBlockPattern = new Regex(@"^\s*-{2,}\s*\)\s*\{", RegexOptions.IgnoreCase);
        private static Regex EndExecutionBeginExecutionBlockPattern = new Regex(@"^\s*-{2,}\s*\}\s*\{", RegexOptions.IgnoreCase);
        private static Regex BeginExecutionBlockPattern = new Regex(@"^\s*-{2,}\s*\{", RegexOptions.IgnoreCase);
        private static Regex BeginEvaluationBlockPattern = new Regex(@"^\s*-{2,}\s*\(", RegexOptions.IgnoreCase);

        // declare local variable
        private static Regex VariableDeclarePattern = new Regex(@"^\s*--\s*var\s*\[\s*([\w_-]+)\s*,\s*(-?\d+)\s*\]", RegexOptions.IgnoreCase);
        // add to local variable
        private static Regex VariableAddPattern = new Regex(@"^\s*--\s*add\s*\[\s*([\w_-]+)\s*,\s*(-?\d+)\s*\]", RegexOptions.IgnoreCase);

        //private const string DateOriginKey = "@DateOrigin";
        public static async Task ExecuteControlFlow(this EtlSettings etlSettings, AthenaControlBlock block, AthenaParserSetting parserSetting)
        {
            if (etlSettings.SourceType != EtlSourceEnum.AmazonAthenaPipes) return;
            var pipesSource = etlSettings.AthenaQueryPipesSource;

            var athenaApi = etlSettings.CreatePipesSourceAthenaAPI();

            foreach (var clearning in parserSetting.Clearings)
            {
                await etlSettings.ClearAthenaTable(clearning.Key, clearning.Value);
            }

            var DateOrigin = DateTime.UtcNow.Date.AddDays(-etlSettings.AthenaQueryPipesSource.DaysAgo).ToString("yyyyMMdd");

            await athenaApi.Execute(block, parserSetting);

            foreach (var table in parserSetting.DroppingTables)
            {
                await athenaApi.DropAthenaTable(table);
            }
        }

        /// <summary>
        /// this method should update the state machine. there should be no async method
        /// </summary>
        /// <param name="context"></param>
        /// <param name="etlSettings"></param>
        /// <param name="parserSetting"></param>
        public static void ExecuteStateMachineQueryContext(this StateMachineQueryContext context)
        {
            var settings = context.settings;
            AthenaParserSetting parserSetting = new AthenaParserSetting()
            {
                Clearings = settings.Clearings.Select(p => new KeyValuePair<string, string>(p.Key, p.Value)).ToList(),
                Caches = settings.Caches.ToDictionary(c => c.Key, c => c),
                Date = settings.Date,
                DateFormat = settings.DateFormat,
                TempDatabase = settings.TempDatabase,
                DefaultExportPath = settings.DefaultExportPath,
                DefaultTableName = settings.DefaultTableName,
                DroppingTables = settings.DroppingTables,
                Partitions = settings.Partitions.Select(p => new KeyValuePair<string, string>(p.Key, p.Value)).ToList(),
                Commands = settings.Commands,
                TempTablePath = settings.TempTablePath,
                Variables = settings.Variables.ToDictionary(v => v.Key, v => v.Value)
            };

            var block = context.raw.ParseAthenaPipes(parserSetting);
            bool seeking = context.result != null; // when result is not null, it is in seeking mode;

            //Console.WriteLine($"@Input Conext State: {(context?.state == null ? "": String.Join("," , context.state.Select(i => i.ToString())))}");

            Console.WriteLine("-------------- Start --------------");
            var result = block.LoadNextStateMachineQuery(parserSetting, new LinkedList<int>(), context, ref seeking);
            Console.WriteLine("--------------  End  --------------");
            //Console.WriteLine($"@Output Conext State: {String.Join(",", context.state.Select(i => i.ToString()))}");

            //Console.WriteLine($"@Output Query `{context.query.query}`");

            //Console.WriteLine($"@Output Variables: {(string.Join(",", context.settings.Variables.Select(kv => $"{kv.Key}->{kv.Value}")))}");

            if (!result.IsLoaded)
            {
                context.Status = "COMPLETED";
                context.state.Clear();
                context.LoadVariables(parserSetting);
                context.query = null;
                context.result = null;
            }
            else
            {
                context.Status = "QUERY";
            }

            // see if there is exception
            try
            {

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.GetType().Name);
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                context.ErrorMessage = ex.Message;
                context.Status = "ERROR";
            }
        }



        private static void LoadVariables(this StateMachineQueryContext context, AthenaParserSetting parserSetting)
        {
            var list = new List<KeyValueEntry>();
            context.settings.Variables = list;
            context.settings.DroppingTables = parserSetting.DroppingTables;
            context.settings.Partitions = parserSetting.Partitions
                .Select(kvp => new KeyValueEntry() { Key = kvp.Key, Value = kvp.Value })
                .ToList();
            context.settings.Commands = parserSetting.Commands;
            context.settings.Clearings = parserSetting.Clearings.Select(kvp => new KeyValueEntry() { Key = kvp.Key, Value = kvp.Value }).ToList();
            foreach (var key in parserSetting.Variables.Keys)
            {
                list.Add(new KeyValueEntry()
                {
                    Key = key,
                    Value = parserSetting.Variables[key]
                });
            }
        }

        private static string FindVariable(this StateMachineQueryContext context, string key)
        {
            return context.settings?.Variables.FirstOrDefault(kv => kv.Key == key)?.Value;
        }

        private static void ReadDropAndPartition(this AthenaParserSetting parserSetting, string query)
        {
            var drops = rgxDropTable.Matches(query);
            foreach (Match drop in drops)
            {
                parserSetting.DroppingTables.Add(drop.Groups[1].Value);
            }
            var partitions = rgxLoadPartition.Matches(query);
            foreach (Match partition in partitions)
            {
                parserSetting.Partitions.Add(new KeyValuePair<string, string>(partition.Groups[1].Value, partition.Groups[2].Value));
            }
            var clearings = rgxClearing.Matches(query);
            //parserSetting.Clearings.Clear(); // the previous tables should not be deleted
            foreach (Match clearing in clearings)
            {
                parserSetting.Clearings.Add(new KeyValuePair<string, string>(clearing.Groups[1].Value, clearing.Groups[2].Value));
            }
            //var batchCommands = rgxBatchCommand.Matches(query);
            //foreach (Match batchCommand in batchCommands)
            //{
            //    parserSetting.Commands.Add(batchCommand.Value);
            //}
            //var lambdaCommands = rgxLambdaCommand.Matches(query);
            //foreach (Match lambdaCommand in lambdaCommands)
            //{
            //    parserSetting.Commands.Add(lambdaCommand.Value);
            //}
        }

        private static async Task<ExecutionBlockResult> Execute(
            this AWSAthenaAPI athena, 
            AthenaControlBlock block, 
            AthenaParserSetting parserSetting)
        {
            if (block is QueryBlock)
            {
                // awsAthenaAPI.GetQueryResults()
                QueryBlock queryBlock = block as QueryBlock;
                var query = queryBlock.Query.ApplyVariables(parserSetting);
                Console.WriteLine(query);
                // add drop table and load partition as real time
                parserSetting.ReadDropAndPartition(query); 
                var results = await athena.GetQueryResults(query);
                if (results.Any() && results.First().Any())
                {
                    var value = results.First().First();
                    if(value is string)
                    {
                        return new ExecutionBlockResult() { Type = EvaluationResultType.String, StringValue = value as string };
                    }
                    else if(value is int)
                    {
                        return new ExecutionBlockResult() { Type = EvaluationResultType.Integer, IntegerValue = (long)(int)value };
                    }
                    else if(value is long)
                    {
                        return new ExecutionBlockResult() { Type = EvaluationResultType.Integer, IntegerValue = (long)value };
                    }
                    else if (value is bool)
                    {
                        return new ExecutionBlockResult() { Type = EvaluationResultType.Boolean, BooleanValue = (bool)value };
                    }
                    else
                    {
                        return new ExecutionBlockResult() { Type = EvaluationResultType.Void };
                    }
                }
                else
                {
                    return new ExecutionBlockResult() { Type = EvaluationResultType.Void };
                }
            }
            else if (block is VariableDeclareBlock)
            {
                VariableDeclareBlock variableDeclare = block as VariableDeclareBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine($"var {variableDeclare.Name} = {variableDeclare.InitialValue}");
                parserSetting.VariableDeclare(variableDeclare.Name, variableDeclare.InitialValue);
                return lastResult;
            }
            else if (block is VariableAddBlock)
            {
                VariableAddBlock variableAdd = block as VariableAddBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine($"{variableAdd.Name} += {variableAdd.AddValue}");
                parserSetting.VariableAdd(variableAdd.Name, variableAdd.AddValue);
                return lastResult;
            }
            else if (block is ExecutionBlock)
            {
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("(");
                foreach (var item in (block as ExecutionBlock).Blocks)
                {
                    lastResult = await athena.Execute(item, parserSetting);
                }
                Console.WriteLine(")");
                return lastResult;
            }
            else if (block is EvaluationBlock)
            {
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("{");
                foreach (var item in (block as EvaluationBlock).Blocks)
                {
                    lastResult = await athena.Execute(item, parserSetting);
                }
                Console.WriteLine("}");
                return lastResult;
            }
            else if (block is WhileBlock)
            {
                WhileBlock whileBlock = block as WhileBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("While");
                while ((await athena.Execute(whileBlock.Condition, parserSetting)).AsBoolean())
                {
                    lastResult = await athena.Execute(whileBlock.Block, parserSetting);
                }
                return lastResult;
            }
            else if (block is ForBlock)
            {
                ForBlock forBlock = block as ForBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine($"For({forBlock.Variable}, {forBlock.From}, {forBlock.To}, {forBlock.Step})");
                for (long i = forBlock.From; i < forBlock.To; i += forBlock.Step)
                {
                    lastResult = await athena.Execute(forBlock.Block, parserSetting.AddVariable(forBlock.Variable, i.ToString()));
                }
                return lastResult;
            }
            else if (block is IfBlock)
            {
                IfBlock ifBlock = block as IfBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("If");
                if((await athena.Execute(ifBlock.If.Condition, parserSetting)).AsBoolean())
                {
                    return await athena.Execute(ifBlock.If.Block, parserSetting);
                }
                foreach(var elseIf in ifBlock.ElseIfs)
                {
                    Console.WriteLine("ElseIf");
                    if ((await athena.Execute(elseIf.Condition, parserSetting)).AsBoolean())
                    {
                        return await athena.Execute(elseIf.Block, parserSetting);
                    }
                }
                if(ifBlock.Else is ElseBlock)
                {
                    Console.WriteLine("Else");
                    return await athena.Execute(ifBlock.Else.Block, parserSetting);
                }
                return lastResult;
            }
            else if (block is SwitchBlock)
            {
                SwitchBlock switchBlock = block as SwitchBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;

                Console.WriteLine("Switch");
                var condition = await athena.Execute(switchBlock.Condition, parserSetting);

                Console.WriteLine("{");
                foreach (var caseBlock in switchBlock.Cases)
                {
                    switch (caseBlock.Type)
                    {
                        case CaseValueType.String:
                            if(caseBlock.StringValue == condition.AsString())
                            {
                                Console.WriteLine($"Case {caseBlock.StringValue}:");
                                return await athena.Execute(caseBlock.Block, parserSetting);
                            }
                            break;
                        case CaseValueType.Integer:
                            if (caseBlock.IntegerValue == condition.AsInteger())
                            {
                                Console.WriteLine($"Case {caseBlock.IntegerValue}:");
                                return await athena.Execute(caseBlock.Block, parserSetting);
                            }
                            break;
                    }
                }

                if(switchBlock.Default is DefaultBlock)
                {
                    Console.WriteLine("Default:");
                    return await athena.Execute(switchBlock.Default.Block, parserSetting);
                }

                Console.WriteLine("}");
                return lastResult;
            }
            else if (block is null)
            {
                // do nothing
                return ExecutionBlockResult.Void;
            }
            else
            {
                throw new Exception("Unexpected Block Type");
            }
        }
        
        
        public static StateMachineExecutionResult LoadNextStateMachineQuery(
            this AthenaControlBlock block,
            AthenaParserSetting parserSetting,
            LinkedList<int> state,
            StateMachineQueryContext context,
            ref bool seeking,
            int level = 0)
        {
            level++;
            Debug.WriteLine($"{"".PadLeft(level * 2, ' ')}Level: {level}, Seeking: {(seeking?"True ":"False")} State: {string.Join("->", state.Select(i => i.ToString()))}, Type: {block.GetType().Name}");
            if (context.state == null || !context.state.Any()) seeking = false;
            if (block is QueryBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(QueryBlock)} {(seeking ? "Seeking" : "")}");
                state.AddLast(0);
                // QueryBlock is usually the end of the 
                if (seeking)
                {
                    if (state.Count == level && state.Last.Value == context.state.Last.Value)
                    {
                        seeking = false; // set not seeking
                        // resume the last run
                        state.RemoveLast();
                        // remove the query and previous context
                        context.query = null;

                        Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(QueryBlock)} {(seeking ? "Seeking" : "")}");
                        return new StateMachineExecutionResult()
                        {
                            ResultType = StateMachineExecutionResultType.QueryResult,
                            QueryResult = context.result,
                        };
                    }
                    else
                    {
                        throw new Exception($"The state does not point to this {nameof(QueryBlock)}");
                    }
                }
                else
                {
                    QueryBlock queryBlock = block as QueryBlock;
                    var query = queryBlock.Query.ApplyVariables(parserSetting);
                    //Console.WriteLine($"Query Block:{{\n" +
                    //    $"{query}\n" +
                    //    $"}}");

                    // add drop table and load partition as real time
                    parserSetting.ReadDropAndPartition(query);

                    // load the state machine with 
                    context.query = new QueryObject()
                    {
                        query = query
                    };

                    context.state = new LinkedList<int>();
                    foreach(int s in state) context.state.AddLast(s);
                    context.LoadVariables(parserSetting);
                    state.RemoveLast();

                    Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(QueryBlock)} {(seeking ? "Seeking" : "")}");
                    return new StateMachineExecutionResult()
                    {
                        Context = context,
                        ResultType = StateMachineExecutionResultType.LoadedQuery
                    };
                }

                
            }
            else if (block is VariableDeclareBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(VariableDeclareBlock)} {(seeking ? "Seeking" : "")}");
                if (seeking)
                {
                    throw new Exception($"{nameof(VariableDeclareBlock)} does not support seeking.");
                }
                else
                {
                    state.AddLast(0);
                    VariableDeclareBlock variableDeclare = block as VariableDeclareBlock;
                    StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;
                    Console.WriteLine($"{"".PadLeft(indent + 1, '\t')}var {variableDeclare.Name} = {variableDeclare.InitialValue}");
                    parserSetting.VariableDeclare(variableDeclare.Name, variableDeclare.InitialValue);
                    state.RemoveLast();

                    Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(VariableDeclareBlock)} {(seeking ? "Seeking" : "")}");
                    return lastResult;
                }
            }
            else if (block is VariableAddBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(VariableAddBlock)} {(seeking ? "Seeking" : "")}");
                if (seeking)
                {
                    throw new Exception($"{nameof(VariableAddBlock)} does not support seeking.");
                }
                else
                {
                    state.AddLast(0);
                    VariableAddBlock variableAdd = block as VariableAddBlock;
                    StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;
                    Console.WriteLine($"{"".PadLeft(indent+1, '\t')}{variableAdd.Name} += {variableAdd.AddValue}");
                    parserSetting.VariableAdd(variableAdd.Name, variableAdd.AddValue);
                    state.RemoveLast();
                    Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(VariableAddBlock)} {(seeking ? "Seeking" : "")}");
                    return lastResult;
                }
            }
            else if (block is ExecutionBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(ExecutionBlock)} {(seeking ? "Seeking" : "")}");

                int target = context.state.PollFirst(), step = 0;
                StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;
                //Console.WriteLine("(");

                //if((block as ExecutionBlock).Blocks.Any(b => b is VariableAddBlock))
                //{
                //    Debugger.Break();
                //}

                foreach (var item in (block as ExecutionBlock).Blocks)
                {
                    if (!seeking ||  (seeking && step == target)) // skip the step if in seeking mode and step == target
                    {
                        state.AddLast(step);
                        lastResult = item.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                        state.RemoveLast();
                    }
                    step++;
                    if (lastResult.IsLoaded)
                    {
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(ExecutionBlock)} {(seeking ? "Seeking" : "")}");
                        return lastResult;
                    }
                }
                //Console.WriteLine(")");
                Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(ExecutionBlock)} {(seeking ? "Seeking" : "")}");
                return lastResult;
            }
            else if (block is EvaluationBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(EvaluationBlock)} {(seeking ? "Seeking" : "")}");

                int target = context.state.PollFirst(), step = 0;
                StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;
                //Console.WriteLine("{");
                foreach (var item in (block as EvaluationBlock).Blocks)
                {
                    if (!seeking || (seeking && step == target)) // skip the step if in seeking mode and step == target
                    {
                        state.AddLast(step);
                        lastResult = item.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                        state.RemoveLast();
                    }
                    step++;
                    if (lastResult.IsLoaded)
                    {
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(EvaluationBlock)} {(seeking ? "Seeking" : "")}");
                        return lastResult;
                    }
                }
                //Console.WriteLine("}");

                Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(EvaluationBlock)} {(seeking ? "Seeking" : "")}");
                return lastResult;
            }
            else if (block is WhileBlock)
            {
                int indent = state.Count;
                

                int target = context.state.PollFirst();
                WhileBlock whileBlock = block as WhileBlock;
                StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}While: seeking = {seeking}, target = {target}");
                if (!seeking || target == 0)
                {
                    Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(WhileBlock)}.Condition {(seeking ? "Seeking" : "")}");
                    state.AddLast(0);
                    lastResult = whileBlock.Condition.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                    state.RemoveLast();
                    Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(WhileBlock)}.Condition {(seeking ? "Seeking" : "")}");
                    if (lastResult.IsLoaded) return lastResult;
                    Console.WriteLine($"First Condition = {lastResult.AsBoolean()}");
                }
                while ((!seeking && lastResult.AsBoolean()) || (seeking && target == 1)) // when result is true or when seeking
                {
                    // this will run for both seeking and running mode
                    Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(WhileBlock)}.Block {(seeking ? "Seeking" : "")}");
                    state.AddLast(1);
                    lastResult = whileBlock.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                    state.RemoveLast();
                    Console.WriteLine($"Exection Rusult = {lastResult.AsBoolean()}");
                    if (lastResult.IsLoaded) return lastResult;
                    state.AddLast(0);
                    lastResult = whileBlock.Condition.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                    state.RemoveLast();
                    Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(WhileBlock)}.Block {(seeking ? "Seeking" : "")}");
                    if (lastResult.IsLoaded) return lastResult;
                    Console.WriteLine($"Next Condition = {lastResult.AsBoolean()}");
                }
                return lastResult;
            }
            else if (block is ForBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(ForBlock)} {(seeking ? "Seeking" : "")}");

                int target = context.state.PollFirst();
                ForBlock forBlock = block as ForBlock;
                StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}For({forBlock.Variable}, {forBlock.From}, {forBlock.To}, {forBlock.Step})");
                long from = forBlock.From;
                if (seeking) // when seeking, if the context contains the variable, we will read initialze the variable based on the context value
                {
                    string fromValue = context.FindVariable(forBlock.Variable);
                    if (fromValue != null)
                    {
                        long.TryParse(parserSetting.Variables[forBlock.Variable], out from);
                    }
                    Console.WriteLine($"{"".PadLeft(indent, '\t')}Seeking: {forBlock.Variable} => {from}");
                }
                for (long i = from; seeking || (i < forBlock.To); i += forBlock.Step) // the for condition is also changed to allowing seeking mode bypass!!!
                {
                    // back track 
                    parserSetting.BackTrackAddVariable(forBlock.Variable, i.ToString());
                    state.AddLast(0);
                    lastResult = forBlock.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                    state.RemoveLast();
                    if (lastResult.IsLoaded) return lastResult;
                    parserSetting.BackTrackRemoveVariable(forBlock.Variable);
                    // the mode should not be seeking when it runs up to here
                    if (seeking) throw new Exception($"The seeking mode should be off at up to here!");
                }
                Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(ForBlock)} {(seeking ? "Seeking" : "")}");
                return lastResult;
            }
            else if (block is IfBlock)
            {
                int indent = state.Count;
                

                int target = context.state.PollFirst(), step = 0;
                IfBlock ifBlock = block as IfBlock;
                StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;

                //Console.WriteLine("If");
                bool hadBranch = false;
                if (!seeking || (seeking && step == target))
                {
                    state.AddLast(step);

                    level++;
                    Debug.WriteLine($"{"".PadLeft(level * 2, ' ')}Level: {level}, Seeking: {(seeking ? "True " : "False")} State: {string.Join("->", state.Select(i => i.ToString()))}, Type: {block.GetType().Name}");


                    int branchTarget = context.state.PollFirst();
                    if(!seeking || (seeking && branchTarget == 0))
                    {
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(IfBlock)}.Condition {(seeking ? "Seeking" : "")}");
                        state.AddLast(0);
                        lastResult = ifBlock.If.Condition.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                        hadBranch = lastResult.AsBoolean();
                        state.RemoveLast();
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(IfBlock)}.Condition {(seeking ? "Seeking" : "")}");
                        if (!lastResult.IsConditionComputed) return lastResult;
                    }
                    if ((!seeking && hadBranch) || (seeking && branchTarget == 1)) // when true or when seeking for the block
                    {
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(IfBlock)}.Block {(seeking ? "Seeking" : "")}");
                        hadBranch = true;
                        state.AddLast(1);
                        lastResult = ifBlock.If.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                        state.RemoveLast();
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(IfBlock)}.Block {(seeking ? "Seeking" : "")}");
                        if (!lastResult.IsConditionComputed) return lastResult;
                    }
                    level--;
                    state.RemoveLast();
                }
                step++;

                if (!hadBranch)
                {
                    foreach (var elseIf in ifBlock.ElseIfs)
                    {
                        
                        //Console.WriteLine("ElseIf");

                        state.AddLast(step);
                        level++;
                        Debug.WriteLine($"{"".PadLeft(level * 2, ' ')}Level: {level}, Seeking: {(seeking ? "True " : "False")} State: {string.Join("->", state.Select(i => i.ToString()))}, Type: {block.GetType().Name}");

                        if (!seeking || (seeking && step == target))
                        {
                            int branchTarget = context.state.PollFirst();

                            if (!seeking || (seeking && branchTarget == 0))
                            {
                                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(ElseIfConditionBlock)}.Condition {(seeking ? "Seeking" : "")}");
                                state.AddLast(0);
                                lastResult = elseIf.Condition.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                                hadBranch = lastResult.AsBoolean();
                                state.RemoveLast();
                                Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(ElseIfConditionBlock)}.Condition {(seeking ? "Seeking" : "")}");
                                if (!lastResult.IsConditionComputed) return lastResult;
                            }
                            if ((!seeking && hadBranch) || (seeking && branchTarget == 1))
                            {
                                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(ElseIfConditionBlock)}.Block {(seeking ? "Seeking" : "")}");
                                hadBranch = true;
                                state.AddLast(1);
                                lastResult = elseIf.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                                state.RemoveLast();
                                Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(ElseIfConditionBlock)}.Block {(seeking ? "Seeking" : "")}");
                                if (!lastResult.IsConditionComputed) return lastResult;
                            }
                        }
                        level--;
                        state.RemoveLast();
                        step++;
                    }
                }

                if (!hadBranch)
                {
                    if (ifBlock.Else is ElseBlock)
                    {

                        state.AddLast(step);
                        level++;
                        //Console.WriteLine("Else");
                        if ((!seeking && !hadBranch) || (seeking && step == target))
                        {
                            Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(ElseBlock)} {(seeking ? "Seeking" : "")}");
                            hadBranch = true;
                            state.AddLast(0);
                            lastResult = ifBlock.Else.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                            state.RemoveLast();
                            Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(ElseBlock)} {(seeking ? "Seeking" : "")}");
                            if (!lastResult.IsConditionComputed) return lastResult;
                        }
                        level--;
                        state.RemoveLast();
                        step++;
                    }
                }

                return lastResult;
            }
            else if (block is SwitchBlock)
            {
                int indent = state.Count;
                Console.WriteLine($"{"".PadLeft(indent, '\t')}@{nameof(SwitchBlock)} {(seeking ? "Seeking" : "")}");

                int target = context.state.PollFirst(), step = 0;

                SwitchBlock switchBlock = block as SwitchBlock;
                StateMachineExecutionResult lastResult = StateMachineExecutionResult.Void;

                Console.WriteLine("Switch");

                if(!seeking || (seeking && step == target))
                {
                    state.AddLast(step);
                    lastResult = switchBlock.Condition.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                    state.RemoveLast();
                    if (!lastResult.IsConditionComputed)
                    {
                        Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(SwitchBlock)} {(seeking ? "Seeking" : "")}");
                        return lastResult;
                    }
                }
                step++;

                Console.WriteLine("{");

                bool caseFound = false;
                foreach (var caseBlock in switchBlock.Cases)
                {
                    if(!seeking || (seeking && step == target))
                    {
                        switch (caseBlock.Type)
                        {
                            case CaseValueType.String:
                                if ((!seeking && caseBlock.StringValue == lastResult.QueryResult.strValue) || (seeking && step == target))
                                {
                                    Console.WriteLine($"{"".PadLeft(indent + 1, '\t')}@{nameof(CaseBlock)} {(seeking ? "Seeking" : "")}");
                                    caseFound = true;
                                    Console.WriteLine($"Case {caseBlock.StringValue}:");
                                    state.AddLast(step);
                                    lastResult = caseBlock.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                                    state.RemoveLast();
                                    Console.WriteLine($"{"".PadLeft(indent + 1, '\t')}#{nameof(CaseBlock)} {(seeking ? "Seeking" : "")}");
                                    if (!lastResult.IsConditionComputed) return lastResult;
                                }
                                break;
                            case CaseValueType.Integer:
                                if ((!seeking && caseBlock.IntegerValue == lastResult.QueryResult.intValue) || (seeking && step == target))
                                {
                                    Console.WriteLine($"{"".PadLeft(indent + 1, '\t')}@{nameof(CaseBlock)} {(seeking ? "Seeking" : "")}");
                                    caseFound = true;
                                    Console.WriteLine($"Case {caseBlock.IntegerValue}:");
                                    state.AddLast(step);
                                    lastResult = caseBlock.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                                    state.RemoveLast();
                                    Console.WriteLine($"{"".PadLeft(indent + 1, '\t')}#{nameof(CaseBlock)} {(seeking ? "Seeking" : "")}");
                                    if (!lastResult.IsConditionComputed) return lastResult;
                                }
                                break;
                        }
                    }
                    step++;
                }

                if (!caseFound  && switchBlock.Default is DefaultBlock)
                {
                    Console.WriteLine("Default:");
                    if(!seeking || (seeking && step == target))
                    {
                        state.AddLast(step);
                        lastResult = switchBlock.Default.Block.LoadNextStateMachineQuery(parserSetting, state, context, ref seeking, level);
                        state.RemoveLast();
                        if (!lastResult.IsConditionComputed)
                        {
                            Console.WriteLine($"{"".PadLeft(indent, '\t')}#{nameof(DefaultBlock)} {(seeking ? "Seeking" : "")}");
                            return lastResult;
                        }
                    }
                    step++;
                }

                Console.WriteLine("}");
                return lastResult;
            }
            else if (block is null)
            {
                // do nothing
                return StateMachineExecutionResult.Void;
            }
            else
            {
                throw new Exception("Unexpected Block Type");
            }
        }

        private static bool IsSameState(this LinkedList<int> state, List<int> value)
        {
            if (state.Count != value.Count) return false;
            IEnumerator<int> i1 = state.GetEnumerator(), i2 = value.GetEnumerator();
            while(i1.MoveNext() && i2.MoveNext())
                if (i1.Current != i2.Current)
                    return false;
            return true;
        }

        private static string ApplyVariables(this string query, AthenaParserSetting parserSetting)
        {
            foreach (var kvp in parserSetting.Variables)
            {
                query = Regex.Replace(query, $@"\{{\s*variable\s*\(\s*{kvp.Key}\s*\)\s*\}}", match => kvp.Value, RegexOptions.IgnoreCase);
                query = Regex.Replace(query, $@"\{{\s*variable\s*\(\s*{kvp.Key}\s*\)\s*([\+\-]?)\s*(\d+)\s*\}}", match => (int.Parse(kvp.Value) + (match.Groups[1].Value == "+" ? 1 : -1) * int.Parse(match.Groups[2].Value)).ToString(), RegexOptions.IgnoreCase);
            }
            query = query.ApplyCaches(parserSetting);
            return query;
        }

        private static AthenaParserSetting AddVariable(this AthenaParserSetting parserSetting, string key, string value)
        {
            parserSetting = parserSetting.Clone();
            parserSetting.Variables.Add(key, value);
            return parserSetting;
        }

        private static void VariableDeclare(this AthenaParserSetting parserSetting, string key, long value)
        {
            if (parserSetting.Variables.ContainsKey(key))
            {
                parserSetting.Variables[key] = value.ToString();
            }
            else
            {
                parserSetting.Variables.Add(key, value.ToString());
            }
        }

        private static void VariableAdd(this AthenaParserSetting parserSetting, string key, long value)
        {
            if (parserSetting.Variables.ContainsKey(key))
            {
                parserSetting.Variables[key] = (long.Parse(parserSetting.Variables[key]) +  value).ToString();
            }
            else
            {
                parserSetting.Variables.Add(key, value.ToString());
            }
        }

        public static ExecutionBlock ParseAthenaPipes(this string query, AthenaParserSetting Debug)
        {
            var parsedLines = query.ParseCommandLines();

            Debug.WriteLine($"Queries Parsed: {parsedLines.Count}");

            ExecutionBlock result = new ExecutionBlock();
            result.Started = true;
            Stack<AthenaControlBlock> stack = new Stack<AthenaControlBlock>();
            stack.Push(result);

            SyntaxExpectationFlags syntaxExpectation = SyntaxExpectationFlags.AnyFlowBlock;

            foreach (var line in parsedLines)
            {
                if (line.IsControl)
                {
                    stack.ProcessControl(ref syntaxExpectation, line, Debug);
                }
                else
                {
                    // always put the QueryBlock in the current one of the stack
                    stack.ProcessQuery(ref syntaxExpectation, line, Debug);
                }
            }

            stack.EndIfBlock(ref syntaxExpectation, parsedLines.LastOrDefault() ?? new QueryLine() { From = 1, To = 1, Value = "" }, Debug);

            if (stack.Count > 1)
            {
                var current = stack.Peek();
                throw new Exception($"Incompleted Block Detected: {current.GetType().Name}");
            }

            result.Completed = true;

            return result;
        }

        private static AthenaControlBlock EndIfBlock(this Stack<AthenaControlBlock> stack, ref SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaParserSetting Debug)
        {
            var current = stack.Peek();
            while(current is IfBlock)
            {
                stack.Pop().DebugPopped(Debug);
                Debug.WriteLine($"If Block Ends Here: {line.From} -> {line.Value}");
                current = stack.Peek();
            }
            syntaxExpectation = stack.Expecting(Debug);
            return stack.Peek();
        }

        private static void ProcessControl(this Stack<AthenaControlBlock> stack, ref SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaParserSetting Debug)
        {
            Match match = null;
            var current = stack.Peek();
            if (VariableDeclarePattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                syntaxExpectation.ExpectsAnyFlowBlock(line);
                VariableDeclareBlock variableDeclare = new VariableDeclareBlock()
                {
                    Name = match.Groups[1].Value,
                    InitialValue = long.Parse(match.Groups[2].Value)
                };
                current.As<IMultipleStatementsBlock>().Blocks.Add(variableDeclare);
                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);
            }
            else if (VariableAddPattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                syntaxExpectation.ExpectsAnyFlowBlock(line);
                VariableAddBlock variableAdd = new VariableAddBlock()
                {
                    Name = match.Groups[1].Value,
                    AddValue = long.Parse(match.Groups[2].Value)
                };
                current.As<IMultipleStatementsBlock>().Blocks.Add(variableAdd);
                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);
            }
            else if (WhileBlockPattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"While: {line.From} -> {line.Value}");

                syntaxExpectation.ExpectsAnyFlowBlock(line);

                WhileBlock whileBlock = new WhileBlock() { LineNumber = line.From };
                current.As<IMultipleStatementsBlock>().Blocks.Add(whileBlock);
                stack.Push(whileBlock);
                stack.Push(whileBlock.Condition);

                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);

                if (match.HasEvaluationStart(1)) whileBlock.Condition.Started = true;
            }
            else if (ForBlock1Pattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"For: {line.From} -> {line.Value}");

                syntaxExpectation.ExpectsAnyFlowBlock(line);

                ForBlock forBlock = new ForBlock() { LineNumber = line.From };
                forBlock.Variable = match.GetStringValue(1);
                forBlock.From = match.GetLongValue(2);
                forBlock.To = match.GetLongValue(3);
                forBlock.Step = 1;
                current.As<IMultipleStatementsBlock>().Blocks.Add(forBlock);
                stack.Push(forBlock);
                stack.Push(forBlock.Block);

                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);

                if (match.HasExecutionStart(4)) forBlock.Block.Started = true;
            }
            else if (ForBlock2Pattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"For: {line.From} -> {line.Value}");

                syntaxExpectation.ExpectsAnyFlowBlock(line);

                ForBlock forBlock = new ForBlock() { LineNumber = line.From };
                forBlock.Variable = match.GetStringValue(1);
                forBlock.From = match.GetLongValue(2);
                forBlock.To = match.GetLongValue(3);
                forBlock.Step = match.GetLongValue(4);
                current.As<IMultipleStatementsBlock>().Blocks.Add(forBlock);
                stack.Push(forBlock);
                stack.Push(forBlock.Block);

                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);

                if (match.HasExecutionStart(5)) forBlock.Block.Started = true;

            }
            else if (IfBlockPattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"If: {line.From} -> {line.Value}");

                syntaxExpectation.ExpectsAnyFlowBlock(line);

                IfBlock ifBlock = new IfBlock() { LineNumber = line.From };
                current.As<IMultipleStatementsBlock>().Blocks.Add(ifBlock);
                stack.Push(ifBlock);
                stack.Push(ifBlock.If);
                stack.Push(ifBlock.If.Condition);

                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);

                if (match.HasEvaluationStart(1)) ifBlock.If.Condition.Started = true;
            }
            else if (ElseIfBlockPattern.CanMatch(out match, line))
            {
                Debug.WriteLine($"ElseIf: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsElseIfOrElseBlock(line);

                ElseIfConditionBlock elseIfConditionBlock = new ElseIfConditionBlock() { LineNumber = line.From };
                current.As<IfBlock>().ElseIfs.Add(elseIfConditionBlock);
                stack.Push(elseIfConditionBlock);
                stack.Push(elseIfConditionBlock.Condition);

                if (match.HasEvaluationStart(1)) elseIfConditionBlock.Condition.Started = true;
            }
            else if (ElseBlockPattern.CanMatch(out match, line))
            {
                Debug.WriteLine($"Else: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsElseIfOrElseBlock(line);

                ElseBlock elseBlock = new ElseBlock() { LineNumber = line.From };
                current.As<IfBlock>().Else = elseBlock;
                stack.Push(elseBlock);
                stack.Push(elseBlock.Block);

                if (match.HasExecutionStart(1)) elseBlock.Block.Started = true;
            }
            else if (SwitchBlockPattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"Switch: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsAnyFlowBlock(line);
                SwitchBlock switchBlock = new SwitchBlock() { LineNumber = line.From };
                current.As<IMultipleStatementsBlock>().Blocks.Add(switchBlock);
                stack.Push(switchBlock);
                stack.Push(switchBlock.Condition);

                current.FillCurrentMultipleStatementsBlock(syntaxExpectation);

                if (match.HasEvaluationStart(1)) switchBlock.Condition.Started = true;
            }
            else if (CaseBlock1Pattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"Case: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsCaseOrDefaultBlock(line);
                CaseBlock caseBlock = new CaseBlock() { LineNumber = line.From };
                caseBlock.Type = CaseValueType.String;
                caseBlock.StringValue = match.GetStringValue(1);
                current.As<SwitchBlock>().Cases.Add(caseBlock);
                current.As<SwitchBlock>().Filled = true;
                stack.Push(caseBlock);
                stack.Push(caseBlock.Block);
                if (match.HasExecutionStart(2)) caseBlock.Block.Started = true;
            }
            else if (CaseBlock2Pattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"Case: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsCaseOrDefaultBlock(line);
                CaseBlock caseBlock = new CaseBlock() { LineNumber = line.From };
                caseBlock.Type = CaseValueType.String;
                caseBlock.StringValue = match.GetStringValue(1);
                current.As<SwitchBlock>().Cases.Add(caseBlock);
                current.As<SwitchBlock>().Filled = true;
                stack.Push(caseBlock);
                stack.Push(caseBlock.Block);
                if (match.HasExecutionStart(2)) caseBlock.Block.Started = true;
            }
            else if (CaseBlock3Pattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"Case: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsCaseOrDefaultBlock(line);
                CaseBlock caseBlock = new CaseBlock() { LineNumber = line.From };
                caseBlock.Type = CaseValueType.String;
                caseBlock.StringValue = match.GetStringValue(1);
                current.As<SwitchBlock>().Cases.Add(caseBlock);
                current.As<SwitchBlock>().Filled = true;
                stack.Push(caseBlock);
                stack.Push(caseBlock.Block);
                if (match.HasExecutionStart(2)) caseBlock.Block.Started = true;
            }
            else if (CaseBlock4Pattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"Case: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsCaseOrDefaultBlock(line);
                CaseBlock caseBlock = new CaseBlock() { LineNumber = line.From };
                caseBlock.Type = CaseValueType.Integer;
                caseBlock.IntegerValue = match.GetLongValue(1);
                current.As<SwitchBlock>().Cases.Add(caseBlock);
                current.As<SwitchBlock>().Filled = true;
                stack.Push(caseBlock);
                stack.Push(caseBlock.Block);
                if (match.HasExecutionStart(2)) caseBlock.Block.Started = true;
            }
            else if (DefaultBlockPattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                Debug.WriteLine($"Default: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsCaseOrDefaultBlock(line);
                DefaultBlock defaultBlock = new DefaultBlock() { LineNumber = line.From };
                current.As<SwitchBlock>().Default = defaultBlock;
                current.As<SwitchBlock>().Filled = true;
                stack.Push(defaultBlock);
                stack.Push(defaultBlock.Block);
                if (match.HasExecutionStart(1)) defaultBlock.Block.Started = true;
            }
            else if (BeginEvaluationBlockPattern.CanMatch(out match, line))
            {
                Debug.WriteLine($"Begin Evaluation : {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsEvaluationStart(line, current);
                current.As<EvaluationBlock>().Started = true;
                syntaxExpectation = SyntaxExpectationFlags.AnyFlowBlock;
            }
            else if (BeginExecutionBlockPattern.CanMatch(out match, line))
            {
                current = stack.EndIfBlock(ref syntaxExpectation, line, Debug);
                syntaxExpectation.ExpectsExecutionStart(line, current);
                if(current is ExecutionBlock)
                {
                    
                    var block = current.As<ExecutionBlock>();
                    if (block.Started)
                    {
                        Debug.WriteLine($"New Execution In Execution: {line.From} -> {line.Value}");
                        // start another ExecutionBlock
                        ExecutionBlock executionBlock = new ExecutionBlock() { LineNumber = line.From, Started = true };
                        block.Blocks.Add(executionBlock);
                        stack.Push(executionBlock);
                        block.Filled = true;
                    }
                    else
                    {
                        Debug.WriteLine($"Execution Started: {line.From} -> {line.Value}");
                        current.As<ExecutionBlock>().Started = true;
                    }
                }
                else if(current is SwitchBlock)
                {
                    Debug.WriteLine($"Switch Start : {line.From} -> {line.Value}");
                    current.As<SwitchBlock>().Started = true;
                }
                else if(current is EvaluationBlock)
                {
                    ExecutionBlock executionBlock = new ExecutionBlock() { LineNumber = line.From, Started = true };

                    var block = current.As<EvaluationBlock>();
                    if (block.Started)
                    {
                        Debug.WriteLine($"New Execution In Evaluation: {line.From} -> {line.Value}");
                        block.Blocks.Add(executionBlock);
                        block.Filled = true;
                    }
                    else
                    {
                        Debug.WriteLine($"New Execution to Fill Evaluation: {line.From} -> {line.Value}");
                        block.Started = true;
                        block.Filled = true;
                        block.Completed = true;
                    }
                    stack.Push(executionBlock);
                }
                else
                {
                    throw new Exception($"Expected Execution Start at line {line.From}: {line.Value}");
                }
            }
            else if (EndEvaluationBeginExecutionBlockPattern.CanMatch(out match, line))
            {
                syntaxExpectation.ExpectsEvaluationEnd(line, current);
                current.As<EvaluationBlock>().Completed = true;
                stack.Pop().DebugPopped(Debug);
                var parent = stack.Peek(); // who can have evaluation? While, If, ElseIf, Switch
                if (parent is WhileBlock)
                {
                    Debug.WriteLine($"End While Evaluation and Start Block: {line.From} -> {line.Value}");
                    parent.As<WhileBlock>().Block.Started = true;
                    stack.Push(parent.As<WhileBlock>().Block);
                }
                else if (parent is IfConditionBlock)
                {
                    Debug.WriteLine($"End If Evaluation and Start Block: {line.From} -> {line.Value}");
                    parent.As<IfConditionBlock>().Block.Started = true;
                    stack.Push(parent.As<IfConditionBlock>().Block);
                }
                else if (parent is ElseIfConditionBlock)
                {
                    Debug.WriteLine($"End ElseIf Evaluation and Start Block: {line.From} -> {line.Value}");
                    parent.As<ElseIfConditionBlock>().Block.Started = true;
                    stack.Push(parent.As<ElseIfConditionBlock>().Block);
                }
                else if (parent is SwitchBlock)
                {
                    // no push, just use itself
                    parent.As<SwitchBlock>().Started = true;
                    Debug.WriteLine($"End Switch Evaluation and Start Block: {line.From} -> {line.Value}");
                }
                else
                {
                    throw new Exception($"Expected Evaluation End and Execution Start at line {line.From}: {line.Value}");
                }
            }
            else if (EndEvaluationBlockPattern.CanMatch(out match, line)) // this must be before EndExecutionBlockPattern
            {
                syntaxExpectation.ExpectsEvaluationEnd(line, current);
                current.As<EvaluationBlock>().Completed = true;
                stack.Pop(); // end of evaluation block handles the state transfer from Condition to Block
                var parent = stack.Peek(); // who can have evaluation? While, If, ElseIf, Switch
                if(parent is WhileBlock)
                {
                    Debug.WriteLine($"End While Evaluation: {line.From} -> {line.Value}");
                    stack.Push(parent.As<WhileBlock>().Block);
                    syntaxExpectation = SyntaxExpectationFlags.ExecutionBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
                }
                else if(parent is IfConditionBlock)
                {
                    Debug.WriteLine($"End If Evaluation: {line.From} -> {line.Value}");
                    stack.Push(parent.As<IfConditionBlock>().Block);
                    syntaxExpectation = SyntaxExpectationFlags.ExecutionBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
                }
                else if (parent is ElseIfConditionBlock)
                {
                    Debug.WriteLine($"End ElseIf Evaluation: {line.From} -> {line.Value}");
                    stack.Push(parent.As<ElseIfConditionBlock>().Block);
                    syntaxExpectation = SyntaxExpectationFlags.ExecutionBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
                }
                else if (parent is SwitchBlock)
                {
                    Debug.WriteLine($"End Switch Evaluation: {line.From} -> {line.Value}");
                    // no push, just use itself
                    syntaxExpectation = SyntaxExpectationFlags.ExecutionBlockStart;
                }
                else
                {
                    throw new Exception($"Expected Evaluation End at line {line.From}: {line.Value}");
                }
            }
            else if (EndExecutionBlockPattern.CanMatch(out match, line))
            {
                syntaxExpectation.ExpectsExecutionEnd(line, current);
                if(current is ExecutionBlock)
                {
                    Debug.WriteLine($"End Execution Block: {line.From} -> {line.Value}");
                    current.As<ExecutionBlock>().Completed = true;
                }
                else if(current is SwitchBlock)
                {
                    Debug.WriteLine($"End Switch Block: {line.From} -> {line.Value}");
                    current.As<SwitchBlock>().Completed = true;
                }
                else
                {
                    throw new Exception($"Expected Execution End at line {line.From}: {line.Value}");
                }
            }
            syntaxExpectation = stack.Expecting(Debug);
            Debug.WriteLine($"Expecting: {DisplayExpectation(syntaxExpectation)}");
        }

        /// <summary>
        /// defines the state transfer of the automata
        /// </summary>
        /// <param name="stack"></param>
        /// <returns></returns>
        private static SyntaxExpectationFlags Expecting(this Stack<AthenaControlBlock> stack, AthenaParserSetting Debug)
        {
            var current = stack.Peek();
            if(current is EvaluationBlock)
            {
                var block = current.As<EvaluationBlock>();
                if (block.Completed)
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else if (!block.Started)
                {
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.EvaluationBlockStart;
                }
                else if (!block.Filled)
                {
                    return SyntaxExpectationFlags.AnyFlowBlock;
                }
                else
                {
                    return SyntaxExpectationFlags.AnyFlowBlock  | SyntaxExpectationFlags.EvaluationBlockEnd;
                }
            }
            else if(current is ExecutionBlock)
            {
                var block = current.As<ExecutionBlock>();
                if (block.Completed)
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else if (!block.Started)
                {
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ExecutionBlockStart;
                }
                else if (!block.Filled)
                {
                    return SyntaxExpectationFlags.AnyFlowBlock;
                }
                else
                {
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ExecutionBlockEnd;
                }
            }
            else if (current is ForBlock)
            {
                var block = current.As<ForBlock>();
                if (block.Block.Completed)
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if(current is SwitchBlock)
            {
                var block = current.As<SwitchBlock>();
                if (block.Completed)
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else if(!block.Started)
                {
                    return SyntaxExpectationFlags.ExecutionBlockStart;
                }
                else
                {
                    return SyntaxExpectationFlags.CaseOrDefaultBlock | SyntaxExpectationFlags.ExecutionBlockEnd;
                }
            }
            else if (current is CaseBlock)
            {
                var block = current.As<CaseBlock>();
                if (block.Block.Completed)
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if (current is DefaultBlock)
            {
                var block = current.As<DefaultBlock>();
                if (block.Block.Completed)
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if (current is IfBlock)
            {
                var block = current.As<IfBlock>();
                if (block.If.Block.Completed)
                {
                    // it can expect ElseIf, Else or Other Flow
                    return SyntaxExpectationFlags.ElseIfOrElseBlock | SyntaxExpectationFlags.AnyFlowBlock;
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if (current is IfConditionBlock)
            {
                var block = current.As<IfConditionBlock>();
                if (block.Block.Completed) // should be when block is completed
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else if (block.Condition.Completed)
                {
                    stack.Push(block.Block);
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ExecutionBlockStart;
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if (current is ElseIfConditionBlock)
            {
                var block = current.As<ElseIfConditionBlock>();
                if (block.Block.Completed) // should be when block is completed
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else if (block.Condition.Completed)
                {
                    stack.Push(block.Block);
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ExecutionBlockStart;
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if (current is ElseBlock)
            {
                var block = current.As<ElseBlock>();
                if (block.Block.Completed) // should be when block is completed
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else if (current is WhileBlock)
            {
                var block = current.As<WhileBlock>();
                if (block.Block.Completed) // should be when block is completed
                {
                    stack.Pop().DebugPopped(Debug);
                    return stack.Expecting(Debug);
                }
                else if (block.Condition.Completed)
                {
                    stack.Push(block.Block);
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ExecutionBlockStart;
                }
                else
                {
                    throw new Exception("Unexpected Case");
                }
            }
            else
            {
                throw new Exception("Unexpected Case");
            }
        }

        private static Type typeSyntaxExpectationFlags = typeof(SyntaxExpectationFlags);
        private static string DisplayExpectation(this SyntaxExpectationFlags value)
        {
            List<string> list = new List<string>();
            foreach(var item in Enum.GetValues(typeSyntaxExpectationFlags))
                if (value.Has((SyntaxExpectationFlags)item)) list.Add(Enum.GetName(typeSyntaxExpectationFlags, item));
            return string.Join(", ", list);
        }

        private static void DebugPopped<T>(this T block, AthenaParserSetting Debug) where T: AthenaControlBlock
        {
            Debug.WriteLine($"Popped: {block.GetType().Name}");
        }
        /// <summary>
        /// fill the block when it is expecting the start but got filled by a block without start
        /// </summary>
        /// <param name="current"></param>
        /// <param name="syntaxExpectation"></param>
        private static void FillCurrentMultipleStatementsBlock(this AthenaControlBlock current, SyntaxExpectationFlags syntaxExpectation)
        {
            if (syntaxExpectation.Has(SyntaxExpectationFlags.EvaluationBlockStart))
            {
                current.As<IMultipleStatementsBlock>().Started = true;
                current.As<IMultipleStatementsBlock>().Completed = true;
                current.As<IMultipleStatementsBlock>().Filled = true;
            }
            if (syntaxExpectation.Has(SyntaxExpectationFlags.ExecutionBlockStart))
            {
                current.As<IMultipleStatementsBlock>().Started = true;
                current.As<IMultipleStatementsBlock>().Completed = true;
                current.As<IMultipleStatementsBlock>().Filled = true;
            }
        }

        private static void ExpectsAnyFlowBlock(this SyntaxExpectationFlags syntaxExpectation, QueryLine line)
        {
            if (syntaxExpectation.HasNot(SyntaxExpectationFlags.AnyFlowBlock)) throw new Exception($"Unexpected Flow Block at Line {line.From}: {line.Value}");
        }

        private static void ExpectsElseIfOrElseBlock(this SyntaxExpectationFlags syntaxExpectation, QueryLine line)
        {
            if (syntaxExpectation.HasNot(SyntaxExpectationFlags.ElseIfOrElseBlock)) throw new Exception($"Unexpected ElseIf or Else Block at Line {line.From}: {line.Value}");
        }

        private static void ExpectsCaseOrDefaultBlock(this SyntaxExpectationFlags syntaxExpectation, QueryLine line)
        {
            if (syntaxExpectation.HasNot(SyntaxExpectationFlags.CaseOrDefaultBlock)) throw new Exception($"Unexpected Case or Default Block at Line {line.From}: {line.Value}");
        }

        private static void ExpectsEvaluationStart(this SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaControlBlock current)
        {
            if (!(current is EvaluationBlock) || current.As<EvaluationBlock>().Started) throw new Exception($"Unexpected Evaluation Start at line: {line.From}: {line.Value}");
        }

        private static void ExpectsExecutionStart(this SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaControlBlock current)
        {
            if (current is ExecutionBlock)
            {
                // it can start another internal block in a block
            }
            else if(current is EvaluationBlock)
            {
                // it can start another internal block in a block
            }
            else if(current is SwitchBlock)
            {
                if (current.As<SwitchBlock>().Started) throw new Exception($"Unexpected Execution Start in Switch Block at line: {line.From}: {line.Value}");
            }
            else
                throw new Exception($"Unexpected Execution Start at line: {line.From}: {line.Value}");
        }

        private static void ExpectsEvaluationEnd(this SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaControlBlock current)
        {
            if (!(current is EvaluationBlock) || !current.As<EvaluationBlock>().Filled || current.As<EvaluationBlock>().Completed) throw new Exception($"Unexpected Evaluation Start at line: {line.From}: {line.Value}");
        }

        private static void ExpectsExecutionEnd(this SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaControlBlock current)
        {
            if (current is ExecutionBlock)
            {
                if (current.As<ExecutionBlock>().Completed) throw new Exception($"Unexpected Execution Start at line: {line.From}: {line.Value}");
            }
            else if (current is SwitchBlock)
            {
                if (current.As<SwitchBlock>().Completed) throw new Exception($"Unexpected Execution Start in Switch Block at line: {line.From}: {line.Value}");
            }
            else
                throw new Exception($"Unexpected Execution Start at line: {line.From}: {line.Value}");
        }

        private static void ProcessQuery(this Stack<AthenaControlBlock> stack, ref SyntaxExpectationFlags syntaxExpectation, QueryLine line, AthenaParserSetting Debug)
        {
            var current = stack.Peek();
            if((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == 0 
                && (syntaxExpectation & SyntaxExpectationFlags.ExecutionBlockStart) == 0
                && (syntaxExpectation & SyntaxExpectationFlags.EvaluationBlockStart) == 0
                )
            {
                throw new Exception($"SQL Query can not be inserted into the Block Type '{current.GetType().Name}'");
            }
            if((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == SyntaxExpectationFlags.AnyFlowBlock)
            {

                if (syntaxExpectation.Has(SyntaxExpectationFlags.EvaluationBlockStart))
                {
                    var block = (current as IMultipleStatementsBlock);
                    block.Blocks.Add(new QueryBlock() { Query = line.Value.ApplyCaches(Debug) });
                    block.Started = true;
                    block.Filled = true;
                    block.Completed = true;
                    Debug.WriteLine($"Query [Fill Evaluation] ({line.From},{line.To}): {line.Value}");
                }
                else if (syntaxExpectation.Has(SyntaxExpectationFlags.ExecutionBlockStart))
                {
                    var block = (current as IMultipleStatementsBlock);
                    block.Blocks.Add(new QueryBlock() { Query = line.Value.ApplyCaches(Debug) });
                    block.Started = true;
                    block.Filled = true;
                    block.Completed = true;
                    Debug.WriteLine($"Query [Fill Execution] ({line.From},{line.To}): {line.Value}");
                }
                else
                {
                    var block = current.As<IMultipleStatementsBlock>();
                    block.Blocks.Add(new QueryBlock() { Query = line.Value.ApplyCaches(Debug) });
                    block.Filled = true;
                    Debug.WriteLine($"Query [Block] ({line.From},{line.To}): {line.Value}");
                }
            }
            else
            {
                throw new Exception($"Unexpected SQL Query for Block Type '{current.GetType().Name}'");
            }
            syntaxExpectation = stack.Expecting(Debug);
            Debug.WriteLine($"Expecting: {DisplayExpectation(syntaxExpectation)}");
        }

        private static Regex rgxDate = new Regex(@"\{date\s*\(\s*\)\}");
        private static Regex rgxDateOffset = new Regex(@"\{date\s*\(\s*(-?\d+)\s*\)\}");

        private static Regex rgxExport = new Regex(@"@export\s*\(\s*\)\s*=", RegexOptions.IgnoreCase);
        private static Regex rgxExportPath = new Regex(@"@export\s*\('(\w+\:\/\/[\w\/\(\)\-]+)'\s*,?\s*((\,?\s*[\w]+='[\w_ ]+')*)\s*\)\s*=", RegexOptions.IgnoreCase);

        private static Regex rgxTemp = new Regex(@"@temp\s*\(\s*'([\w_]+)'\s*\)\s*=", RegexOptions.IgnoreCase);
        private static Regex rgxTempRef = new Regex(@"\{\s*temp\s*\(\s*'([\w_]+)'\s*\)\s*\}", RegexOptions.IgnoreCase);

        private static Regex rgxDropTable = new Regex(@"-- *\$Context\.DropTable\(([^\(^\(]+)\)", RegexOptions.IgnoreCase);
        private static Regex rgxLoadPartition = new Regex(@"-- *\$Context\.LoadPartition\(([^\(^\(]+)\).From\(([^\(^\(]+)\)", RegexOptions.IgnoreCase);
        private static Regex rgxClearing = new Regex(@"-- *\$Context\.Clear\(([^\(^\(]+)\).At\(([^\(^\(]+)\)", RegexOptions.IgnoreCase);

        //public static Regex rgxBatchCommand = new Regex(@"-- *#Batch +([\w\:\-_]+) +([\w\:\-_]+) +([\w\:\-_]+) +(input|parameters)=([^\n]+)", RegexOptions.IgnoreCase);
        //public static Regex rgxLambdaCommand = new Regex(@"-- *#Lambda +([\w\:\-_]+) +(input)=([^\n]+)", RegexOptions.IgnoreCase);

        public static string ApplyCaches(this string value, AthenaParserSetting parserSetting)
        {
            var caches = parserSetting.Caches;
            foreach (var key in caches.Keys)
            {
                var cache = caches[key];
                Regex regexCTAS = new Regex($@"@{key}\((\w[\w_]*)\)\s*=", RegexOptions.IgnoreCase);
                Regex regexRef = new Regex($@"@{key}\((\w[\w_]*)\)\s*=", RegexOptions.IgnoreCase);
                value = regexCTAS.Replace(value, match => {
                    var tableName = $"{cache.Database}.{match.Groups[1].Value}";
                    var s3Path = $"{cache.S3Path}{parserSetting.Date.ToString(parserSetting.DateFormat)}/{match.Groups[1].Value}/";
                    //parserSetting.Clearings.Add(new KeyValuePair<string, string>(tableName, s3Path));
                    return $@"Create Table {tableName} 
-- Table will be dropped and cleared before CTAS query
-- $Context.Clear({tableName}).At({s3Path})
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{s3Path}'
    ) As ";
                });
                value = regexRef.Replace(value, match => $"{cache.Database}.{match.Groups[1].Value}");
            }



            value = rgxDate.Replace(value, match => $"{parserSetting.Date.ToString(parserSetting.DateFormat)}");
            value = rgxDateOffset.Replace(value, match => $"{parserSetting.Date.AddDays(int.Parse(match.Groups[1].Value)).ToString(parserSetting.DateFormat)}");


            value = rgxExport.Replace(value, match =>
            {
                var tablePath = $"Ex{Guid.NewGuid().ToString().Replace("-", "")}";
                var tempTable = $"{parserSetting.TempDatabase}.{tablePath}";
                var path = $"{parserSetting.DefaultExportPath}{tablePath}/";
                //parserSetting.Clearings.Add(new KeyValuePair<string, string>(tempTable, path));
                //parserSetting.DroppingTables.Add(tempTable);
                var CTAS = $@"Create Table {tempTable} 
-- Etl Tool Will Run: DROP TABLE IF EXISTS {tempTable}
-- $Context.DropTable({tempTable})
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{path}'
    ) As ";
                return CTAS;
            });

            value = rgxExportPath.Replace(value, match =>
            {
                var tablePath = $"Ex{Guid.NewGuid().ToString().Replace("-", "")}";
                var tempTable = $"{parserSetting.TempDatabase}.{tablePath}";
                var path = match.Groups[1].Value;
                if (!path.EndsWith("/")) path += "/";
                path += $"{tablePath}/";
                //parserSetting.Clearings.Add(new KeyValuePair<string, string>(tempTable, path));
                //parserSetting.DroppingTables.Add(tempTable);
                //parserSetting.Partitions.Add(new KeyValuePair<string, string>(match.Groups[2].Value, path));
                var CTAS = $@"Create Table {tempTable} 
-- Etl Tool Will Run: DROP TABLE IF EXISTS {tempTable}
-- $Context.DropTable({tempTable})
-- Etl Tool Will Run: ALTER TABLE {parserSetting.DefaultTableName} DROP IF EXISTS PARTITION ({match.Groups[2].Value})
-- Etl Tool Will Run: ALTER TABLE {parserSetting.DefaultTableName} ADD IF NOT EXISTS PARTITION ({match.Groups[2].Value}) LOCATION '{path}'
-- $Context.LoadPartition({match.Groups[2].Value}).From({path})
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{path}'
    ) As ";
                return CTAS;
            });

            value = rgxTemp.Replace(value, match =>
            {
                var tempTable = $"{parserSetting.TempDatabase}.{match.Groups[1].Value}";
                var tablePath = $"{parserSetting.TempTablePath}{match.Groups[1].Value}/";
                var CTAS = $@"Create Table {tempTable} 
-- Etl Tool Will Run: DROP TABLE IF EXISTS {tempTable}
-- $Context.DropTable({tempTable})
-- $Context.Clear({tempTable}).At({tablePath})
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{tablePath}'
    ) As ";
                //parserSetting.Clearings.Add(new KeyValuePair<string, string>(tempTable, tablePath));
                //parserSetting.DroppingTables.Add(tempTable);
                return CTAS;
            });

            value = rgxTempRef.Replace(value, match => $"{parserSetting.TempDatabase}.{match.Groups[1].Value}");

            return value;
        }

        public static List<QueryLine> ParseCommandLines(this string query)
        {
            // get lines
            var lines = query.Split(new char[] { '\n' });
            List<QueryLine> results = new List<QueryLine>();
            StringBuilder queryBuilder = null;
            int lineIndex = 0, queryStartLineIndex = 0;
            foreach (var line in lines)
            {
                lineIndex++;
                if (ControlFlowPattern.IsMatch(line))
                {
                    if (queryBuilder != null)
                    {
                        results.Add(new QueryLine()
                        {
                            IsControl = false,
                            Value = queryBuilder.ToString(),
                            From = queryStartLineIndex,
                            To = lineIndex - 1
                        });
                        queryBuilder = null;
                    }
                    results.Add(new QueryLine()
                    {
                        IsControl = true,
                        Value = line,
                        From = lineIndex,
                        To = lineIndex
                    });
                }
                else if (CommentLinePattern.IsMatch(line))
                {
                    // ignore the line
                }
                else if (EmptyLinePattern.IsMatch(line))
                {
                    // ignore the line
                }
                else
                {
                    if (queryBuilder == null)
                    {
                        queryBuilder = new StringBuilder();
                        queryStartLineIndex = lineIndex;
                    }
                    queryBuilder.AppendLine(line);
                }
            }
            if (queryBuilder != null)
            {
                results.Add(new QueryLine()
                {
                    IsControl = false,
                    Value = queryBuilder.ToString(),
                    From = queryStartLineIndex,
                    To = lineIndex
                });
                queryBuilder = null;
            }
            return results;
        }



        private static bool HasEvaluationStart(this Match match, int index)
        {
            return match.Groups.Count > index && match.Groups[index].Value == "(";
        }

        private static bool HasExecutionStart(this Match match, int index)
        {
            return match.Groups.Count > index && match.Groups[index].Value == "{";
        }

        private static bool CanMatch(this Regex regex, out Match result, QueryLine line)
        {
            result = regex.Match(line.Value);
            return result.Success;
        }

        public static T As<T>(this AthenaControlBlock value) where T : class => value as T;
        private static long GetLongValue(this Match match, int index) => long.Parse(match.Groups[index].Value);
        private static string GetStringValue(this Match match, int index) => match.Groups[index].Value;

        private static bool Has(this SyntaxExpectationFlags value, SyntaxExpectationFlags flag)
        {
            return (value & flag) == flag;
        }
        private static bool HasNot(this SyntaxExpectationFlags value, SyntaxExpectationFlags flag)
        {
            return (value & flag) == 0;
        }

        public static string ToQueryString<T>(this T block, int indent = 0) where T: AthenaControlBlock
        {
            StringBuilder stb = new StringBuilder();
            if (block is EvaluationBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- (");
                foreach (var item in (block as EvaluationBlock).Blocks)
                {
                    stb.Append(item.ToQueryString(indent + 1));
                }
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- )");
            }
            else if (block is ExecutionBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- {{");
                foreach (var item in (block as ExecutionBlock).Blocks)
                {
                    stb.Append(item.ToQueryString(indent + 1));
                }
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- }}");
            }
            else if (block is IfBlock)
            {
                stb.Append((block as IfBlock).If.ToQueryString(indent + 1));
                foreach (var item in (block as IfBlock).ElseIfs)
                {
                    stb.Append(item.ToQueryString(indent + 1));
                }
                if ((block as IfBlock).Else is ElseBlock)
                {
                    stb.Append((block as IfBlock).Else.ToQueryString(indent + 1));
                }
            }
            else if (block is IfConditionBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- If ");
                stb.Append((block as IfConditionBlock).Condition.ToQueryString(indent));
                stb.Append((block as IfConditionBlock).Block.ToQueryString(indent));
            }
            else if (block is ElseIfConditionBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- ElseIf ");
                stb.Append((block as ElseIfConditionBlock).Condition.ToQueryString(indent));
                stb.Append((block as ElseIfConditionBlock).Block.ToQueryString(indent));
            }
            else if (block is ElseBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- Else ");
                stb.Append((block as ElseBlock).Block.ToQueryString(indent));
            }
            else if (block is WhileBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- While ");
                stb.Append((block as WhileBlock).Condition.ToQueryString(indent));
                stb.Append((block as WhileBlock).Block.ToQueryString(indent));
            }
            else if (block is SwitchBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- Switch ");
                stb.Append((block as SwitchBlock).Condition.ToQueryString(indent));
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- {{");
                foreach (var item in (block as SwitchBlock).Cases)
                {
                    stb.Append(item.ToQueryString(indent + 1));
                }
                if ((block as SwitchBlock).Default is DefaultBlock)
                {
                    stb.Append((block as SwitchBlock).Default.ToQueryString(indent + 1));
                }
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- }}");
            }
            else if (block is CaseBlock)
            {
                var caseBlock = block as CaseBlock;
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- Case ({caseBlock.QueryValue()}) ");
                stb.Append((block as CaseBlock).Block.ToQueryString(indent));
            }
            else if (block is DefaultBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- Default ");
                stb.Append((block as DefaultBlock).Block.ToQueryString(indent));
            }
            else if (block is ForBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- {(block as ForBlock).QueryExpression()} ");
                stb.Append((block as ForBlock).Block.ToQueryString(indent));
            }
            else if (block is QueryBlock)
            {
                stb.AppendLine((block as QueryBlock).Query);
            }
            else if (block is VariableDeclareBlock)
            {
                var variableDeclare = block as VariableDeclareBlock;
                stb.AppendLine($"-- var[{variableDeclare.Name}, {variableDeclare.InitialValue}]");
            }
            else if (block is VariableAddBlock)
            {
                var variableAdd = block as VariableAddBlock;
                stb.AppendLine($"-- add[{variableAdd.Name}, {variableAdd.AddValue}]");
            }
            return stb.ToString();
        }

        private static Regex NoneExmptyLine = new Regex(@"\S+");
        public static string StripEmptyLines(this string value)
        {
            return string.Join("\n", 
                value
                .Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
                .Where(line => NoneExmptyLine.IsMatch(line))
                );
        }
    }


    public class QueryLine
    {
        public bool IsControl { get; set; }
        public string Value { get; set; }
        public int From { get; set; }
        public int To { get; set; }
    }

    public abstract class AthenaControlBlock
    {
        public string Name { get; set; }
        public int LineNumber { get; set; }
    }

    public abstract class AthenaControlFlowBlock: AthenaControlBlock
    {

    }

    public class VariableDeclareBlock: AthenaControlFlowBlock
    {
        public string VariableName { get; set; }
        public long InitialValue { get; set; }
    }

    public class VariableAddBlock: AthenaControlFlowBlock
    {
        public string VariableName { get; set; }
        public long AddValue { get; set; }
    }
 
    public enum SyntaxExpectationFlags
    {
        AnyFlowBlock = 1,
        ElseIfOrElseBlock = 2,
        CaseOrDefaultBlock = 4,

        ExecutionBlockStart = 128,
        ExecutionBlockEnd = 256,
        EvaluationBlockStart = 512,
        EvaluationBlockEnd = 1024,
        SwitchRegionStart = 2048,
        SwitchRegionEnd = 4096,
    }

    public interface IRegionBlock
    {
        bool Started { get; set; }
        bool Filled { get; set; }
        bool Completed { get; set; }
    }

    public interface IMultipleStatementsBlock: IRegionBlock
    {
        List<AthenaControlFlowBlock> Blocks { get; set; }
    }

    public interface IConditionBlock
    {
        EvaluationBlock Condition { get; set; }
        ExecutionBlock Block { get; set; }
    }

    public class EvaluationBlock: AthenaControlBlock, IMultipleStatementsBlock
    {
        public EvaluationBlock()
        {
            Name = nameof(EvaluationBlock);
        }
        public List<AthenaControlFlowBlock> Blocks { get; set; } = new List<AthenaControlFlowBlock>();
        public bool Started { get; set; }
        public bool Completed { get; set; }
        public bool Filled { get; set; }
    }

    public class ExecutionBlock : AthenaControlFlowBlock, IMultipleStatementsBlock
    {
        public ExecutionBlock()
        {
            Name = nameof(ExecutionBlock);
        }
        public List<AthenaControlFlowBlock> Blocks { get; set; } = new List<AthenaControlFlowBlock>();
        public bool Started { get; set; }
        public bool Completed { get; set; }
        public bool Filled { get; set; }
    }

    public class QueryBlock : AthenaControlFlowBlock
    {
        public QueryBlock()
        {
            Name = nameof(QueryBlock);
        }

        public string ParsedQuery()
        {
            return "";
        }
        public string Query { get; set; }
    }

    public class IfBlock: AthenaControlFlowBlock
    {
        public IfBlock()
        {
            Name = nameof(IfBlock);
        }
        public IfConditionBlock If { get; set; } = new IfConditionBlock();
        public List<ElseIfConditionBlock> ElseIfs { get; set; } = new List<ElseIfConditionBlock>();
        public ElseBlock Else { get; set; }
    }

    public class IfConditionBlock: AthenaControlBlock, IConditionBlock
    {
        public IfConditionBlock()
        {
            Name = nameof(IfConditionBlock);
        }
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }

    public class ElseIfConditionBlock: AthenaControlBlock, IConditionBlock
    {
        public ElseIfConditionBlock()
        {
            Name = nameof(ElseIfConditionBlock);
        }
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }
    public class ElseBlock: AthenaControlBlock
    {
        public ElseBlock()
        {
            Name = nameof(ElseBlock);
        }
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }


    public class ForBlock : AthenaControlFlowBlock
    {
        public ForBlock()
        {
            Name = nameof(ForBlock);
        }
        public string Variable { get; set; }
        public long From { get; set; }
        public long To { get; set; }
        public long Step { get; set; } = -1;
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
        public string QueryExpression()
        {
            return $"For({Variable}, {From}, {To}, {Step})";
        }
    }
    
    public class WhileBlock: AthenaControlFlowBlock, IConditionBlock
    {
        public WhileBlock()
        {
            Name = nameof(WhileBlock);
        }
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }

    public class SwitchBlock : AthenaControlFlowBlock, IRegionBlock
    {
        public SwitchBlock()
        {
            Name = nameof(SwitchBlock);
        }
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public List<CaseBlock> Cases { get; set; } = new List<CaseBlock>();
        public DefaultBlock Default { get; set; }
        public bool Started { get; set; }
        public bool Completed { get; set; }
        public bool Filled { get; set; }
    }

    public class CaseBlock: AthenaControlBlock
    {
        public CaseBlock()
        {
            Name = nameof(CaseBlock);
        }
        public CaseValueType Type { get; set; }
        public string StringValue { get; set; }
        public long IntegerValue { get; set; }
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
        public string QueryValue()
        {
            switch (Type)
            {
                case CaseValueType.Integer:
                    return IntegerValue.ToString();
                case CaseValueType.String:
                    return StringValue;
                default:
                    return "N/A";
            }
        }
    }

    public enum CaseValueType
    {
        Integer,
        String
    }

    public class DefaultBlock: AthenaControlBlock
    {
        public DefaultBlock()
        {
            Name = nameof(DefaultBlock);
        }
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }

    public enum AthenaControlFlowNodeType
    {
        Block,
        WhileCondition,
        WhileBlock,
        ForBlock,
        If,
        IfCondition,
        IfBlock,
        ElseIfCondition,
        ElseIfBlock,
        ElseBlock,
        SwitchCondition,
        CaseBlock,
        DefaultBlock,
    }

    public class AthenaParserSetting
    {
        private StringBuilder stringBuilder = new StringBuilder();
        public void WriteLine(string value)
        {
            stringBuilder.AppendLine(value);
        }

        public void Write(string value)
        {
            stringBuilder.Append(value);
        }

        public override string ToString()
        {
            return stringBuilder.ToString();
        }

        public Dictionary<string, CacheSetting> Caches { get; set; } = new Dictionary<string, CacheSetting>();
        public DateTime Date { get; set; }
        public string DateFormat { get; set; }
        public string DefaultTableName { get; set; }
        public string DefaultExportPath { get; set; }
        public List<KeyValuePair<string,string>> Partitions { get; set; } = new List<KeyValuePair<string, string>>();
        public List<KeyValuePair<string, string>> Clearings { get; set; } = new List<KeyValuePair<string, string>>();
        public List<string> Commands { get; set; } = new List<string>();

        public string TempDatabase { get; set; }
        public string TempTablePath { get; set; }
        public List<string> DroppingTables { get; set; } = new List<string>();
        public Dictionary<string, string> Variables { get; set; } = new Dictionary<string, string>();

        public void BackTrackAddVariable(string key, string value)
        {
            if (Variables.ContainsKey(key))
                Variables[key] = value;
            else
                Variables.Add(key, value);
        }

        public void BackTrackRemoveVariable(string key)
        {
            Variables.Remove(key);
        }

        public AthenaParserSetting Clone() =>
            new AthenaParserSetting()
            {
                stringBuilder = new StringBuilder(stringBuilder.ToString()),
                Caches = Caches,
                Date = Date,
                DateFormat = DateFormat,
                DefaultTableName = DefaultTableName,
                DefaultExportPath = DefaultExportPath,
                Partitions = Partitions,
                Clearings = Clearings,
                TempDatabase = TempDatabase,
                DroppingTables = DroppingTables,
                Variables = Variables.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
            };
    }

    public class StateMachineSettings
    {
        public List<CacheSetting> Caches { get; set; }
        public DateTime Date { get; set; }
        public string DateFormat { get; set; }
        public string DefaultTableName { get; set; }
        public string DefaultExportPath { get; set; }
        public List<KeyValueEntry> Partitions { get; set; }
        public List<KeyValueEntry> Clearings { get; set; }
        public string TempDatabase { get; set; }
        public string TempTablePath { get; set; }
        public List<string> DroppingTables { get; set; }
        public List<string> Commands { get; set; }
        public List<KeyValueEntry> Variables { get; set; }
    }

    public class KeyValueEntry
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }

    public class CacheSetting
    {
        public string Key { get; set; }
        public string Database { get; set; }
        public string S3Path { get; set; }
    }

    public enum EvaluationResultType
    {
        Boolean,
        Integer,
        String,
        Void
    }

    public class ExecutionBlockResult
    {
        public EvaluationResultType Type { get; set; }
        public string StringValue { get; set; }
        public long IntegerValue { get; set; }
        public bool BooleanValue { get; set; }

        public bool AsBoolean()
        {
            switch (Type)
            {
                case EvaluationResultType.Boolean:
                    return BooleanValue;
                case EvaluationResultType.Integer:
                    return IntegerValue > 0L;
                case EvaluationResultType.String:
                    return !string.IsNullOrEmpty(StringValue);
                default:
                    return false;
            }
        }

        public string AsString()
        {
            switch (Type)
            {
                case EvaluationResultType.Boolean:
                    return BooleanValue.ToString();
                case EvaluationResultType.Integer:
                    return IntegerValue.ToString();
                case EvaluationResultType.String:
                    return StringValue ?? "";
                default:
                    return "";
            }
        }

        public long AsInteger()
        {
            switch (Type)
            {
                case EvaluationResultType.Boolean:
                    return BooleanValue ? 1L : 0L;
                case EvaluationResultType.Integer:
                    return IntegerValue;
                case EvaluationResultType.String:
                    return string.IsNullOrEmpty(StringValue) ? 0 : 1;
                default:
                    return 0;
            }
        }

        public static ExecutionBlockResult Void = new ExecutionBlockResult() { Type = EvaluationResultType.Void };
    }


    // this should be retrieved from the state machine query executor
    public class StateMachineQueryResult
    {
        public string strValue { get; set; }
        public bool boolValue { get; set; }
        public int intValue { get; set; }
        public bool success { get; set; }
        public string error { get; set; }
        public string state { get; set; }

        public void SetValue(string value)
        {
            strValue = value;
            boolValue = value != null && value.ToLower() == "true";
            int parsing;
            if(int.TryParse(value, out parsing)) intValue = parsing;
            success = true;
            state = "SUCCESS";
        }
        public void SetValue(int value)
        {
            strValue = value.ToString();
            boolValue = value > 0;
            intValue = value;
            success = true;
            state = "SUCCESS";
        }
        public void SetValue(bool value)
        {
            strValue = value.ToString();
            boolValue = value;
            intValue = value ? 1 : 0;
            success = true;
            state = "SUCCESS";
        }
        public void SetValue(long value)
        {
            strValue = value.ToString();
            boolValue = value > 0L;
            intValue = (int)value;
            success = true;
            state = "SUCCESS";
        }

        public readonly static StateMachineQueryResult Void = new StateMachineQueryResult()
        {
            strValue = "",
            boolValue = false,
            intValue = 0,
            success = true,
            error = "",
            state = "SUCCESS"
        };

        public readonly static StateMachineQueryResult True = new StateMachineQueryResult()
        {
            strValue = "true",
            boolValue = true,
            intValue = 1,
            success = true,
            error = "",
            state = "SUCCESS"
        };

        public readonly static StateMachineQueryResult False = new StateMachineQueryResult()
        {
            strValue = "false",
            boolValue = false,
            intValue = 0,
            success = true,
            error = "",
            state = "SUCCESS"
        };

        public static StateMachineQueryResult Integer(int value) => new StateMachineQueryResult()
        {
            strValue = $"{value}",
            boolValue = value > 0,
            intValue = value,
            success = true,
            error = "",
            state = "SUCCESS"
        };
    }

    public class StateMachineQueryContext
    {
        /// <summary>
        /// string 
        /// </summary>
        public StateMachineQueryResult result { get; set; }
        /// <summary>
        /// the raw query in the context
        /// </summary>
        public string raw { get; set; }
        public string rawS3 { get; set; }
        /// <summary>
        /// the key-values stored in the context
        /// </summary>
        // public List<QueryContextEntry> context { get; set; }
        /// <summary>
        /// this indicates the position of the running context
        /// </summary>
        public int position { get; set; }
        public LinkedList<int> state { get; set; }
        //public string FindVariable(string key)
        //{
        //    return context.Where(c => c.key == key).Select(c => c.value).FirstOrDefault();
        //}
        public QueryObject query { get; set;}
        /// <summary>
        /// this should hold most of the values in the ParserSetting
        /// </summary>
        public StateMachineSettings settings { get; set; }

        /// <summary>
        ///  State should be 1 of the 3: QUERY COMPLETED ERROR
        /// </summary>
        public string Status { get; set; }
        public string ErrorMessage { get; set; }
    }

    public class StateMachineQueryEntry
    {
        public string EtlName { get; set; }
        public string Date { get; set; }
    }

    public class QueryObject
    {
        public string query { get; set; }
    }

    public class StateMachineExecutionResult
    {
        public StateMachineExecutionResultType ResultType { get; set; }
        public StateMachineQueryResult QueryResult { get; set; }
        public StateMachineQueryContext Context { get; set; }
        public static readonly StateMachineExecutionResult Void = new StateMachineExecutionResult()
        {
            ResultType = StateMachineExecutionResultType.BlockReturn,
            QueryResult = StateMachineQueryResult.Void,
            Context = null,
        };

        public bool IsConditionComputed
        {
            get {
                switch (ResultType)
                {
                    case StateMachineExecutionResultType.QueryResult:
                        if (!QueryResult.success) throw new Exception($"Query Failed: {QueryResult.error}");
                        return true;
                    case StateMachineExecutionResultType.LoadedQuery:
                    case StateMachineExecutionResultType.None:
                    case StateMachineExecutionResultType.BlockReturn:
                    default:
                        return false;
                }
            }
        }

        public bool IsLoaded { get => ResultType == StateMachineExecutionResultType.LoadedQuery; }

        public bool AsBoolean()
        {
            switch (ResultType)
            {
                case StateMachineExecutionResultType.QueryResult:
                    return QueryResult.boolValue;
                case StateMachineExecutionResultType.LoadedQuery:
                case StateMachineExecutionResultType.None:
                case StateMachineExecutionResultType.BlockReturn:
                default:
                    return false;
            }
        }

        public long AsInteger()
        {
            switch (ResultType)
            {
                case StateMachineExecutionResultType.QueryResult:
                    return QueryResult.intValue;
                case StateMachineExecutionResultType.LoadedQuery:
                case StateMachineExecutionResultType.None:
                case StateMachineExecutionResultType.BlockReturn:
                default:
                    return 0;
            }
        }
        public string AsString()
        {
            switch (ResultType)
            {
                case StateMachineExecutionResultType.QueryResult:
                    return QueryResult.strValue;
                case StateMachineExecutionResultType.LoadedQuery:
                case StateMachineExecutionResultType.None:
                case StateMachineExecutionResultType.BlockReturn:
                default:
                    return "";
            }
        }
    }

    public enum StateMachineExecutionResultType
    {
        None,
        QueryResult,
        LoadedQuery,
        BlockReturn
    }

    public class QueryContextEntry
    {
        public string key { get; set; }
        public string value { get; set; }
    }

    public static class LinkedListExtensions
    {
        public static T PollFirst<T>(this LinkedList<T> linkedList)
        {
            if (linkedList == null || linkedList.First == null) return default(T);
            var item = linkedList.First.Value;
            linkedList.RemoveFirst();
            return item;
        }

        public static T PollLast<T>(this LinkedList<T> linkedList)
        {
            if (linkedList == null || linkedList.Last == null) return default(T);
            var item = linkedList.Last.Value;
            linkedList.RemoveLast();
            return item;
        }
    }
}
