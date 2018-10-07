﻿using ArangoDB.Client.Data;
using ArangoDB.Client.Utility;
using Remotion.Linq;
using Remotion.Linq.Parsing.ExpressionVisitors;
using Remotion.Linq.Parsing.Structure.IntermediateModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ArangoDB.Client.Query.Clause
{
    public class ShortestPathExpressionNode : MethodCallExpressionNodeBase, IQuerySourceExpressionNode
    {
        public static readonly MethodInfo[] SupportedMethods = new[]
                                                           {
                                                                LinqUtility.GetSupportedMethod(()=>ShortestPathQueryableExtensions.ShortestPath<object,object>(null, "", "")),
                                                                LinqUtility.GetSupportedMethod(()=>ShortestPathQueryableExtensions.ShortestPath<object,object>(null, ()=>"", ()=>""))
                                                           };

        public Expression StartVertex { get; private set; }

        public Expression TargetVertex { get; private set; }

        public ConstantExpression VertextType { get; private set; }
        public ConstantExpression EdgeType { get; private set; }

        string identifier;

        private readonly ResolvedExpressionCache<Expression> _resolvedAdaptedSelector;

        public ShortestPathExpressionNode(MethodCallExpressionParseInfo parseInfo, Expression startVertex, Expression targetVertex)
            : base(parseInfo)
        {
            StartVertex = startVertex;
            TargetVertex = targetVertex;

            identifier = Guid.NewGuid().ToString("N");

            var genericTypes = parseInfo.ParsedExpression.Type.GenericTypeArguments[0].GenericTypeArguments;
            VertextType = Expression.Constant(genericTypes[0]);
            EdgeType = Expression.Constant(genericTypes[1]);

            _resolvedAdaptedSelector = new ResolvedExpressionCache<Expression>(this);
        }

        public override Expression Resolve(ParameterExpression inputParameter, Expression expressionToBeResolved, ClauseGenerationContext clauseGenerationContext)
        {
            LinqUtility.CheckNotNull("inputParameter", inputParameter);
            LinqUtility.CheckNotNull("expressionToBeResolved", expressionToBeResolved);

            var resolvedSelector = GetResolvedAdaptedSelector(clauseGenerationContext);
            var resolved = ReplacingExpressionVisitor.Replace(inputParameter, Expression.Parameter(resolvedSelector.Type, identifier), expressionToBeResolved);
            return resolved;
        }

        protected override void ApplyNodeSpecificSemantics(QueryModel queryModel, ClauseGenerationContext clauseGenerationContext)
        {
            LinqUtility.CheckNotNull("queryModel", queryModel);

            var traversalClause = new TraversalClause(StartVertex, TargetVertex, identifier);

            queryModel.BodyClauses.Add(traversalClause);

            clauseGenerationContext.AddContextInfo(this, traversalClause);

            //queryModel.SelectClause.Selector = GetResolvedAdaptedSelector(clauseGenerationContext);
        }

        public Expression GetResolvedAdaptedSelector(ClauseGenerationContext clauseGenerationContext)
        {
            return _resolvedAdaptedSelector.GetOrCreate(
                r =>
                {
                    var shortestPathDataType = typeof(ShortestPathData<,>).MakeGenericType(new Type[] { VertextType.Value as Type, EdgeType.Value as Type });

                    var constr = ReflectionUtils.GetConstructors(shortestPathDataType).ToList()[0];
                    var newExpression =
                        Expression.Convert(
                        Expression.New(constr), shortestPathDataType)
                        ;

                    return r.GetResolvedExpression(newExpression, Expression.Parameter(shortestPathDataType, "dummy"), clauseGenerationContext);
                });
        }
    }
}
