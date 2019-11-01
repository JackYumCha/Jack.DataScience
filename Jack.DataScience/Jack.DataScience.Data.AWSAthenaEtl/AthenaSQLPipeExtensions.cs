using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Data.AWSAthenaEtl
{


    public static class AthenaSQLPipeExtensions
    {


        private static Regex ControlFlowPattern = new Regex(@"^ *-{2,} *(while *\(?|for *\( *[\w_]+ *, *-?\d+ *, *-?\d+ *\) *\{?|for *\( *[\w_]+ *, *-?\d+ *, *-?\d+ *, *-?\d+ *\) *\{?|if *\(?|elseif *\(?|else *\{?|switch *\(?|case *\( *`[^`]+` *\) *\{?|case *\( *""[^""]+"" *\) *\{?|case *\( *'[^']+' *\) *\{?|case *\( *-?\d+ *\) *\{?|default *\{?|\) *\{|\} *\{|\{|\)|\}|\()");

        private static Regex CommentLinePattern = new Regex("^ *--");
        private static Regex EmptyLinePattern = new Regex(@"^\s*$");

        private static Regex WhileBlockPattern = new Regex(@"^ *-{2,} *while *(\(?)", RegexOptions.IgnoreCase);
        private static Regex ForBlock1Pattern = new Regex(@"^ *-{2,} *for *\( *([\w_]+) *, *(-?\d+) *, *(-?\d+) *\) *(\{?)", RegexOptions.IgnoreCase);
        private static Regex ForBlock2Pattern = new Regex(@"^ *-{2,} *for *\( *([\w_]+) *, *(-?\d+) *, *(-?\d+) *, *(-?\d+) *\) *(\{?)", RegexOptions.IgnoreCase);
        private static Regex IfBlockPattern = new Regex(@"^ *-{2,} *if *(\(?)", RegexOptions.IgnoreCase);
        private static Regex ElseIfBlockPattern = new Regex(@"^ *-{2,} *elseif *(\(?)", RegexOptions.IgnoreCase);
        private static Regex ElseBlockPattern = new Regex(@"^ *-{2,} *else *(\{?)", RegexOptions.IgnoreCase);
        private static Regex SwitchBlockPattern = new Regex(@"^ *-{2,} *switch *(\(?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock1Pattern = new Regex(@"^ *-{2,} *case *\( *`([^`]+)` *\) *(\{?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock2Pattern = new Regex(@"^ *-{2,} *case *\( *'([^']+)' *\) *(\{?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock3Pattern = new Regex(@"^ *-{2,} *case *\( *""([^""]+)"" *\) *(\{?)", RegexOptions.IgnoreCase);
        private static Regex CaseBlock4Pattern = new Regex(@"^ *-{2,} *case *\( *(-?\d+) *\) *(\{?)", RegexOptions.IgnoreCase);
        private static Regex DefaultBlockPattern = new Regex(@"^ *-{2,} *default *(\{?)", RegexOptions.IgnoreCase);
        private static Regex EndEvaluationBlockPattern = new Regex(@"^ *-{2,} *\)", RegexOptions.IgnoreCase);
        private static Regex EndExecutionBlockPattern = new Regex(@"^ *-{2,} *\}", RegexOptions.IgnoreCase);
        private static Regex EndEvaluationBeginExecutionBlockPattern = new Regex(@"^ *-{2,} *\) *\{", RegexOptions.IgnoreCase);
        private static Regex EndExecutionBeginExecutionBlockPattern = new Regex(@"^ *-{2,} *\} *\{", RegexOptions.IgnoreCase);
        private static Regex BeginExecutionBlockPattern = new Regex(@"^ *-{2,} *\{", RegexOptions.IgnoreCase);
        private static Regex BeginEvaluationBlockPattern = new Regex(@"^ *-{2,} *\(", RegexOptions.IgnoreCase);

        public static ExecutionBlock ParseAthenaPipes(this string query)
        {
            var parsedLines = query.ParseCommandLines();

            Debug.WriteLine($"Queries Parsed: {parsedLines.Count}");

            ExecutionBlock result = new ExecutionBlock();
            Stack<AthenaControlBlock> stack = new Stack<AthenaControlBlock>();
            stack.Push(result);

            SyntaxExpectationFlags syntaxExpectation = SyntaxExpectationFlags.AnyFlowBlock;

            foreach (var line in parsedLines)
            {
                if (line.IsControl)
                {
                    stack.ProcessControl(ref syntaxExpectation, line);
                }
                else
                {
                    // always put the QueryBlock in the current one of the stack
                    stack.ProcessQuery(ref syntaxExpectation, line);
                }
            }

            return result;
        }

        private static void ProcessControl(this Stack<AthenaControlBlock> stack, ref SyntaxExpectationFlags syntaxExpectation, QueryLine line)
        {
            Match match = null;
            var current = stack.Peek();

            if (WhileBlockPattern.CanMatch(out match, line))
            {
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
                Debug.WriteLine($"If: {line.From} -> {line.Value}");

                syntaxExpectation.ExpectsAnyFlowBlock(line);

                IfBlock ifBlock = new IfBlock() { LineNumber = line.From };
                current.As<IMultipleStatementsBlock>().Blocks.Add(ifBlock);
                stack.Push(ifBlock);
                stack.Push(ifBlock.If);
                stack.Push(ifBlock.If.Condition);

                if(match.HasEvaluationStart(1))
                    syntaxExpectation = SyntaxExpectationFlags.EvaluationBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
                else
                {
                    ifBlock.If.Condition.Started = true;
                    syntaxExpectation = SyntaxExpectationFlags.AnyFlowBlock;
                }
            }
            else if (ElseIfBlockPattern.CanMatch(out match, line))
            {
                Debug.WriteLine($"ElseIf: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsElseIfOrElseBlock(line);

                ElseIfConditionBlock elseIfConditionBlock = new ElseIfConditionBlock() { LineNumber = line.From };
                current.As<IfBlock>().ElseIfs.Add(elseIfConditionBlock);
                stack.Push(elseIfConditionBlock);
                stack.Push(elseIfConditionBlock.Condition);

                if (match.HasEvaluationStart(1))
                    syntaxExpectation = SyntaxExpectationFlags.EvaluationBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
                else
                {
                    elseIfConditionBlock.Condition.Started = true;
                    syntaxExpectation = SyntaxExpectationFlags.AnyFlowBlock;
                }
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
                Debug.WriteLine($"Switch: {line.From} -> {line.Value}");
                syntaxExpectation.ExpectsAnyFlowBlock(line);
                SwitchBlock switchBlock = new SwitchBlock() { LineNumber = line.From };
                current.As<IMultipleStatementsBlock>().Blocks.Add(switchBlock);
                stack.Push(switchBlock);
                stack.Push(switchBlock.Condition);

                if (match.HasEvaluationStart(1)) switchBlock.Condition.Started = true;
            }
            else if (CaseBlock1Pattern.CanMatch(out match, line))
            {
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
            else if (EndEvaluationBlockPattern.CanMatch(out match, line))
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
            else if (EndEvaluationBeginExecutionBlockPattern.CanMatch(out match, line))
            {
                syntaxExpectation.ExpectsEvaluationEnd(line, current);
                current.As<EvaluationBlock>().Completed = true;
                stack.Pop();
                var parent = stack.Peek(); // who can have evaluation? While, If, ElseIf, Switch
                if (parent is WhileBlock)
                {
                    Debug.WriteLine($"End While Evaluation and Start Block: {line.From} -> {line.Value}");
                    stack.Push(parent.As<WhileBlock>().Block);
                }
                else if (parent is IfConditionBlock)
                {
                    Debug.WriteLine($"End If Evaluation and Start Block: {line.From} -> {line.Value}");
                    stack.Push(parent.As<IfConditionBlock>().Block);
                }
                else if (parent is ElseIfConditionBlock)
                {
                    Debug.WriteLine($"End ElseIf Evaluation and Start Block: {line.From} -> {line.Value}");
                    stack.Push(parent.As<ElseIfConditionBlock>().Block);
                }
                else if (parent is SwitchBlock)
                {
                    // no push, just use itself
                    Debug.WriteLine($"End Switch Evaluation and Start Block: {line.From} -> {line.Value}");
                }
                else
                {
                    throw new Exception($"Expected Evaluation End and Execution Start at line {line.From}: {line.Value}");
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
            syntaxExpectation = stack.Expecting();
        }

        /// <summary>
        /// defines the state transfer of the automata
        /// </summary>
        /// <param name="stack"></param>
        /// <returns></returns>
        private static SyntaxExpectationFlags Expecting(this Stack<AthenaControlBlock> stack)
        {
            var current = stack.Peek();
            if(current is EvaluationBlock)
            {
                var block = current.As<EvaluationBlock>();
                if (block.Completed)
                {
                    stack.Pop();
                    return stack.Expecting();
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
                    stack.Pop();
                    return stack.Expecting();
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
                    return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ExecutionBlockStart;
                }
            }
            else if(current is SwitchBlock)
            {
                var block = current.As<SwitchBlock>();
                if (block.Completed)
                {
                    stack.Pop();
                    return stack.Expecting();
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
                    stack.Pop();
                    return stack.Expecting();
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
                    stack.Pop();
                    return stack.Expecting();
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
                    stack.Pop();
                    return stack.Expecting();
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
                var block = current.As<IfConditionBlock>();
                if (block.Block.Completed) // should be when block is completed
                {
                    stack.Pop();
                    return stack.Expecting();
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
                var block = current.As<IfConditionBlock>();
                if (block.Block.Completed) // should be when block is completed
                {
                    stack.Pop();
                    return stack.Expecting();
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
                    stack.Pop();
                    return stack.Expecting();
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

        private static void ProcessQuery(this Stack<AthenaControlBlock> stack, ref SyntaxExpectationFlags syntaxExpectation, QueryLine line)
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
                var block = current.As<IMultipleStatementsBlock>();
                block.Blocks.Add(new QueryBlock() { Query = line.Value });
                block.Filled = true;
            }
            else if (syntaxExpectation.Has(SyntaxExpectationFlags.EvaluationBlockStart))
            {
                var block = (current as IMultipleStatementsBlock);
                block.Blocks.Add(new QueryBlock() { Query = line.Value });
                block.Started = true;
                block.Filled = true;
                block.Completed = true;
                syntaxExpectation = stack.Expecting();
            }
            else if (syntaxExpectation.Has(SyntaxExpectationFlags.ExecutionBlockStart))
            {
                var block = (current as IMultipleStatementsBlock);
                block.Blocks.Add(new QueryBlock() { Query = line.Value });
                block.Started = true;
                block.Filled = true;
                block.Completed = true;
            }
            else
            {
                throw new Exception($"Unexpected SQL Query for Block Type '{current.GetType().Name}'");
            }
            syntaxExpectation = stack.Expecting();
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
}
