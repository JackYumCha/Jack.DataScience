using Jack.DataScience.Data.AWSAthena;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.AWSAthenaEtl
{


    public static class AthenaSQLPipeExtensions
    {


        private static Regex ControlFlowPattern = new Regex(@"^\s*-{2,}\s*(while\s*\(?|for\s*\(\s*[\w_]+\s*,\s*-?\d+\s*,\s*-?\d+\s*\)\s*\{?|for\s*\(\s*[\w_]+\s*,\s*-?\d+\s*,\s*-?\d+\s*,\s*-?\d+\s*\)\s*\{?|if\s*\(?|elseif\s*\(?|else\s*\{?|switch\s*\(?|case\s*\(\s*`[^`]+`\s*\)\s*\{?|case\s*\(\s*""[^""]+""\s*\)\s*\{?|case\s*\(\s*'[^']+'\s*\)\s*\{?|case\s*\(\s*-?\d+\s*\)\s*\{?|default\s*\{?|\)\s*\{|\}\s*\{|\{|\)|\}|\()");

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


        public static async Task ExecuteControlFlow(this EtlSettings etlSettings, AthenaControlBlock block, AthenaParserSetting parserSetting)
        {
            if (etlSettings.SourceType != EtlSourceEnum.AmazonAthenaPipes) return;
            var pipesSource = etlSettings.AthenaQueryPipesSource;

            var athenaApi = etlSettings.CreatePipesSourceAthenaAPI();

            foreach (var clearning in parserSetting.Clearings)
            {
                await etlSettings.ClearAthenaTable(clearning.Key, clearning.Value);
            }

            await athenaApi.Execute(block, new Dictionary<string, string>());

            foreach (var table in parserSetting.DroppingTables)
            {
                await athenaApi.DropAthenaTable(table);
            }
        }


        private static async Task<ExecutionBlockResult> Execute(this AWSAthenaAPI athena, AthenaControlBlock block, Dictionary<string, string> variables = null)
        {
            if (block is QueryBlock)
            {
                // awsAthenaAPI.GetQueryResults()
                QueryBlock queryBlock = block as QueryBlock;
                var query = queryBlock.Query.ApplyVariables(variables);
                Console.WriteLine(query);
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
            else if (block is ExecutionBlock)
            {
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("(");
                foreach (var item in (block as ExecutionBlock).Blocks)
                {
                    lastResult = await athena.Execute(item, variables);
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
                    lastResult = await athena.Execute(item, variables);
                }
                Console.WriteLine("}");
                return lastResult;
            }
            else if (block is WhileBlock)
            {
                WhileBlock whileBlock = block as WhileBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("While");
                while ((await athena.Execute(whileBlock.Condition)).AsBoolean())
                {
                    lastResult = await athena.Execute(whileBlock.Block, variables);
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
                    lastResult = await athena.Execute(forBlock.Block, variables.AddVariable(forBlock.Variable, i.ToString()));
                }
                return lastResult;
            }
            else if (block is IfBlock)
            {
                IfBlock ifBlock = block as IfBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;
                Console.WriteLine("If");
                if((await athena.Execute(ifBlock.If.Condition)).AsBoolean())
                {
                    return await athena.Execute(ifBlock.If.Block);
                }
                foreach(var elseIf in ifBlock.ElseIfs)
                {
                    Console.WriteLine("ElseIf");
                    if ((await athena.Execute(elseIf.Condition)).AsBoolean())
                    {
                        return await athena.Execute(elseIf.Block);
                    }
                }
                if(ifBlock.Else is ElseBlock)
                {
                    Console.WriteLine("Else");
                    return await athena.Execute(ifBlock.Else.Block);
                }
                return lastResult;
            }
            else if (block is SwitchBlock)
            {
                SwitchBlock switchBlock = block as SwitchBlock;
                ExecutionBlockResult lastResult = ExecutionBlockResult.Void;

                Console.WriteLine("Switch");
                var condition = await athena.Execute(switchBlock.Condition);

                Console.WriteLine("{");
                foreach (var caseBlock in switchBlock.Cases)
                {
                    switch (caseBlock.Type)
                    {
                        case CaseValueType.String:
                            if(caseBlock.StringValue == condition.AsString())
                            {
                                Console.WriteLine($"Case {caseBlock.StringValue}:");
                                return await athena.Execute(caseBlock.Block);
                            }
                            break;
                        case CaseValueType.Integer:
                            if (caseBlock.IntegerValue == condition.AsInteger())
                            {
                                Console.WriteLine($"Case {caseBlock.IntegerValue}:");
                                return await athena.Execute(caseBlock.Block);
                            }
                            break;
                    }
                }

                if(switchBlock.Default is DefaultBlock)
                {
                    Console.WriteLine("Default:");
                    return await athena.Execute(switchBlock.Default.Block);
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


        private static string ApplyVariables(this string query, Dictionary<string, string> variables)
        {
            if(variables is Dictionary<string, string>)
            {
                foreach(var kvp in variables)
                {
                    query = Regex.Replace(query, $@"variable\(\s*{kvp.Key}\s*\)", match => kvp.Value, RegexOptions.IgnoreCase);
                }
            }
            return query;
        }

        private static Dictionary<string, string> AddVariable(this Dictionary<string, string> variables, string key, string value)
        {
            if (variables == null)
                return new Dictionary<string, string>() { { key, value } };
            else
            {
                var dict = variables.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                dict.Add(key, value);
                return dict;
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

            if (WhileBlockPattern.CanMatch(out match, line))
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
                    parserSetting.Clearings.Add(new KeyValuePair<string, string>(tableName, s3Path));
                    return $@"Create Table {tableName} 
-- Table will be dropped and cleared before CTAS query
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{s3Path}'
    ) As ";
                });
                value = regexRef.Replace(value, match => $"{cache.Database}.{match.Groups[1].Value}");
            }

            Regex date = new Regex(@"date\(\s*\)");
            Regex dateOffset = new Regex(@"date\(\s*(-?\d+)\s*\)");

            value = date.Replace(value, match => $"{parserSetting.Date.ToString(parserSetting.DateFormat)}");
            value = dateOffset.Replace(value, match => $"{parserSetting.Date.AddDays(int.Parse(match.Groups[1].Value)).ToString(parserSetting.DateFormat)}");

            Regex export = new Regex(@"@export\(\s*\)", RegexOptions.IgnoreCase);
            Regex exportPath = new Regex(@"@export\('(\w+\:\/\/[\w\/\(\)\-]+)'\s*,?\s*((\,?\s*[\w]+='[\w_ ]+')*)\s*\)", RegexOptions.IgnoreCase);

            value = export.Replace(value, match =>
            {
                var tablePath = $"Ex{Guid.NewGuid().ToString().Replace("-", "")}";
                var tempTable = $"{parserSetting.TempDatabase}.{tablePath}";
                var path = $"{parserSetting.DefaultExportPath}{tablePath}/";
                parserSetting.Clearings.Add(new KeyValuePair<string, string>(tempTable, path));
                parserSetting.DroppingTables.Add(tempTable);
                var CTAS = $@"Create Table {tempTable} 
-- Etl Tool Will Run: DROP TABLE IF EXISTS {tempTable}
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{path}'
    ) As";
                return CTAS;
            });

            value = exportPath.Replace(value, match =>
            {
                var tablePath = $"Ex{Guid.NewGuid().ToString().Replace("-", "")}";
                var tempTable = $"{parserSetting.TempDatabase}.{tablePath}";
                var path = match.Groups[1].Value;
                if (!path.EndsWith("/")) path += "/";
                path += $"{tablePath}/";
                parserSetting.Clearings.Add(new KeyValuePair<string, string>(tempTable, path));
                parserSetting.DroppingTables.Add(tempTable);
                parserSetting.Partitions.Add(new KeyValuePair<string, string>(match.Groups[2].Value, path));
                var CTAS = $@"Create Table {tempTable} 
-- Etl Tool Will Run: DROP TABLE IF EXISTS {tempTable}
-- Etl Tool Will Run: ALTER TABLE {parserSetting.DefaultTableName} ADD IF NOT EXISTS ({match.Groups[2].Value}) LOCATION '{path}'
    With (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = '{path}'
    ) As";
                return CTAS;
            });

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
            if(block is EvaluationBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- (");
                foreach(var item in (block as EvaluationBlock).Blocks) {
                    stb.Append(item.ToQueryString(indent + 1));
                }
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- )");
            }
            else if(block is ExecutionBlock)
            {
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- {{");
                foreach (var item in (block as ExecutionBlock).Blocks)
                {
                    stb.Append(item.ToQueryString(indent + 1));
                }
                stb.AppendLine($"{"".PadRight(indent * 2, ' ')}-- }}");
            }
            else if(block is IfBlock)
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

        public string TempDatabase { get; set; }
        public List<string> DroppingTables { get; set; } = new List<string>();
        
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
}
