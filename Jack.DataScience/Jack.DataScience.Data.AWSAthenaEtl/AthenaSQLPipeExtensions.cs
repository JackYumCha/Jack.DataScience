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

            Debugger.Break();
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
                if ((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == 0) throw new Exception($"Unexpected While Block at Line {line.From}");
                WhileBlock whileBlock = new WhileBlock() { LineNumber = line.From };
                current.As<IMultipleStatementsBlock>().Blocks.Add(whileBlock);
                stack.Push(whileBlock);
                stack.Push(whileBlock.Condition);
                if (match.HasEvaluationStart(1))
                    syntaxExpectation = SyntaxExpectationFlags.EvaluationBlockStart;
                else
                    syntaxExpectation = SyntaxExpectationFlags.AnyFlowBlock;
            }
            else if (ForBlock1Pattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == 0) throw new Exception($"Unexpected For Block at Line {line.From}");

            }
            else if (ForBlock2Pattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == 0) throw new Exception($"Unexpected For Block at Line {line.From}");

            }
            else if (IfBlockPattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == 0) throw new Exception($"Unexpected If Block at Line {line.From}");

            }
            else if (ElseIfBlockPattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.ElseIfOrElseBlock) == 0) throw new Exception($"Unexpected ElseIf Block at Line {line.From}");

            }
            else if (ElseBlockPattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.ElseIfOrElseBlock) == 0) throw new Exception($"Unexpected Else Block at Line {line.From}");

            }
            else if (SwitchBlockPattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.AnyFlowBlock) == 0) throw new Exception($"Unexpected Switch Block at Line {line.From}");

            }
            else if (CaseBlock1Pattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.CaseOrDefaultBlock) == 0) throw new Exception($"Unexpected Case Block at Line {line.From}");

            }
            else if (DefaultBlockPattern.CanMatch(out match, line))
            {
                if ((syntaxExpectation & SyntaxExpectationFlags.CaseOrDefaultBlock) == 0) throw new Exception($"Unexpected Default Block at Line {line.From}");

            }
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
                (current as IMultipleStatementsBlock).Blocks.Add(new QueryBlock() { Query = line.Value });
            }
            else if ((syntaxExpectation & SyntaxExpectationFlags.EvaluationBlockStart) == SyntaxExpectationFlags.EvaluationBlockStart)
            {
                var block = (current as IMultipleStatementsBlock);
                block.Blocks.Add(new QueryBlock() { Query = line.Value });
                block.Started = true;
                block.Filled = true;
                block.Completed = true;
                stack.Pop();
                // next could be If() While() Switch()
                var parent = stack.Peek();
                if(parent is IConditionBlock) // While(){, If(){,  ElseIf(){
                {
                    stack.Push(parent.As<IConditionBlock>().Block);
                    syntaxExpectation = SyntaxExpectationFlags.ExecutionBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
                }
                else if(parent is SwitchBlock) // Switch() { 
                {
                    syntaxExpectation = SyntaxExpectationFlags.SwitchRegionStart; // it must have a start {
                }
            }
            else if ((syntaxExpectation & SyntaxExpectationFlags.ExecutionBlockStart) == SyntaxExpectationFlags.ExecutionBlockStart)
            {
                var block = (current as IMultipleStatementsBlock);
                block.Blocks.Add(new QueryBlock() { Query = line.Value });
                block.Started = true;
                block.Filled = true;
                block.Completed = true;
                stack.Pop();
                syntaxExpectation = stack.ExpecationOnExecutionComplete();
            }
            else
            {
                throw new Exception($"Unexpected SQL Query for Block Type '{current.GetType().Name}'");
            }
        }

        private static SyntaxExpectationFlags ExpecationOnEvaluationComplete(this Stack<AthenaControlBlock> stack)
        {
            var current = stack.Peek();
            if (current is IConditionBlock) // While(){, IfCondition(){,  ElseIfCondition(){
            {
                stack.Push(current.As<IConditionBlock>().Block);
                return SyntaxExpectationFlags.ExecutionBlockStart | SyntaxExpectationFlags.AnyFlowBlock;
            }
            else if (current is SwitchBlock) // Switch() { 
            {
                return SyntaxExpectationFlags.SwitchRegionStart; // it must have a start {
            }
            else
            {
                throw new Exception("No Syntax Expectation avaliable.");
            }
        }

        private static SyntaxExpectationFlags ExpecationOnExecutionComplete(this Stack<AthenaControlBlock> stack)
        {
            var current = stack.Peek();
            if (current is IfConditionBlock)
            {
                stack.Pop(); // remove IfConditionBlock
                return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ElseIfOrElseBlock;
            }
            else if (current is ElseIfConditionBlock)
            {
                stack.Pop(); // remove ElseIfConditionBlock
                return SyntaxExpectationFlags.AnyFlowBlock | SyntaxExpectationFlags.ElseIfOrElseBlock;
            }
            else if (current is ElseBlock)
            {
                stack.Pop(); // remove ElseBlock
                stack.Pop(); // remove IfBlock
                return SyntaxExpectationFlags.AnyFlowBlock;
            }
            else if (current is WhileBlock)
            {
                stack.Pop();
                return SyntaxExpectationFlags.AnyFlowBlock;
            }
            else if (current is ForBlock)
            {
                stack.Pop(); // remove ForBlock
                return SyntaxExpectationFlags.AnyFlowBlock;
            }
            else if (current is CaseBlock)
            {
                stack.Pop(); // remove CaseBlock
                return SyntaxExpectationFlags.CaseOrDefaultBlock | SyntaxExpectationFlags.SwitchRegionEnd; // for Switch(){
            }
            else if (current is DefaultBlock)
            {
                stack.Pop(); // remove DefaultBlock
                return SyntaxExpectationFlags.CaseOrDefaultBlock | SyntaxExpectationFlags.SwitchRegionEnd; // for Switch(){
            }
            else
            {
                throw new Exception("No Syntax Expectation avaliable.");
            }
        }

        public static List<QueryLine> ParseCommandLines(this string query)
        {
            // get lines
            var lines = query.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
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
        public int LineNumber { get; set; }
    }

    public abstract class AthenaControlFlowBlock: AthenaControlBlock
    {

    }

    public class PipeBlock : AthenaControlFlowBlock
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
        public List<AthenaControlFlowBlock> Blocks { get; set; } = new List<AthenaControlFlowBlock>();
        public bool Started { get; set; }
        public bool Completed { get; set; }
    }

    public class ExecutionBlock : AthenaControlFlowBlock, IMultipleStatementsBlock
    {
        public List<AthenaControlFlowBlock> Blocks { get; set; } = new List<AthenaControlFlowBlock>();
        public bool Started { get; set; }
        public bool Completed { get; set; }
    }

    public class QueryBlock : AthenaControlFlowBlock
    {
        public string Query { get; set; }
    }

    public class IfBlock: AthenaControlFlowBlock
    {
        public IfConditionBlock If { get; set; } = new IfConditionBlock();
        public List<ElseIfConditionBlock> ElseIfs { get; set; } = new List<ElseIfConditionBlock>();
        public ElseBlock Else { get; set; }
    }

    public class IfConditionBlock: AthenaControlBlock, IConditionBlock
    {
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }

    public class ElseIfConditionBlock: AthenaControlBlock, IConditionBlock
    {
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }
    public class ElseBlock: AthenaControlBlock
    {
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }


    public class ForBlock : AthenaControlFlowBlock
    {
        public string Variable { get; set; }
        public int From { get; set; }
        public int To { get; set; }
        public int Step { get; set; } = -1;
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }
    
    public class WhileBlock: AthenaControlFlowBlock, IConditionBlock
    {
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public ExecutionBlock Block { get; set; } = new ExecutionBlock();
    }

    public class SwitchBlock : AthenaControlFlowBlock, IRegionBlock
    {
        public EvaluationBlock Condition { get; set; } = new EvaluationBlock();
        public List<CaseBlock> Cases { get; set; } = new List<CaseBlock>();
        public DefaultBlock Default { get; set; }
        public bool Started { get; set; }
        public bool Completed { get; set; }
    }

    public class CaseBlock: AthenaControlBlock
    {
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
