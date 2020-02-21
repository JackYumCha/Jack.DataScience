using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Jack.DataScience.Serverless
{

    public class Step
    {
        public int Index { get; set; }
        public Action Action { get; set; }
        public Func<bool> Function { get; set; }
        public void Execute()
        {
            
        }
    }

    public enum StepModeEnum
    {
        Action, Function, BoolCondition, IfBlock, If, ElseIf, Else, SwitchBlock, SwitchCondition, SwitchCase, SwitchElse, WhileBlock, WhileCondition
    }

    public class Resumable: IDisposable
    {
        public int WaitSeconds { get; set; }
        public int Index { get; set; }
        private List<Step> Steps { get; set; }

        public Resumable SubContext()
        {
            // there should be a suspend method that store the current stack and terminate the 
            return new Resumable();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public IClauseIf If(Expression<Func<bool>> expression, Action action)
        {
            return (IClauseIf)new object();
        }

        public ContextInt Int(string name)
        {
            return new ContextInt();
        }

        public void Run(Action action)
        {
            action.Invoke();
        }

        /// <summary>
        /// tell the context to wait for seconds before resume
        /// </summary>
        /// <param name="seconds"></param>
        public void Wait(int seconds)
        {
             
        }



        public void Execute()
        {
            Steps[Index].Execute();
        }
    }


    public interface IClauseIf
    {
        IClauseIf ElseIf();
        void Else();
    }

    public class ContextInt
    {
        private int value;
        public static ContextInt operator +(ContextInt a, ContextInt b)
        {
            return new ContextInt() { value = a.value + b.value };
        }
        public static bool operator ==(ContextInt a, ContextInt b)
        {
            return a.value == b.value;
        }
        public static bool operator !=(ContextInt a, ContextInt b)
        {
            return a.value != b.value;
        }
        public static Expression<Func<bool>> operator ==(ContextInt a, int b)
        {
            return () => a.value == b;
        }
        public static Expression<Func<bool>> operator !=(ContextInt a, int b)
        {
            return () => a.value != b;
        }
        public static bool operator ==(int a, ContextInt b)
        {
            return a == b.value;
        }
        public static bool operator !=(int a, ContextInt b)
        {
            return a != b.value;
        }
        public static implicit operator ContextInt(int value)
        {
            return new ContextInt { value = value };
        }
    }

    public class ContextString
    {

    }

    public class ContextBool
    {

    }

    public class ContextStruct<T> where T : class, new()
    {
        internal T value;
        internal ContextStruct()
        {
            value = new T();
        }
        public T Value { get => value; }
    }

    class MyContext: Resumable
    {
        public int x { get; set; }
        public MyContext()
        {
            // this will auto initialize the ContextInt, ContextBool, etc
            // x = Int(nameof(x));
        }
    }

    class Test
    {

        public void Test01()
        {
            using(var ctx = new MyContext())
            {
                ctx.If(() => ctx.x == 2, () =>
                {
                   
                });
                
                using (var sub1 = ctx.SubContext())
                {
                    
                }

                
            }
        }
    }
}
