using System.Collections.Generic;
using System.Windows;
using System.Windows.Media;
using System.Windows.Media.Media3D;

namespace Jack.DataScience.WPF.Controls
{
    public static class TreeHelper
    {

        public static IEnumerable<T> Parents<T>(this DependencyObject source) where T : DependencyObject
        {
            if (source == null) yield break;
            HashSet<DependencyObject> parents = new HashSet<DependencyObject>();
            var logicParent = LogicalTreeHelper.GetParent(source);
            if(logicParent != null) parents.Add(logicParent);
            if(source is Visual || source is Visual3D)
            {
                var visualParent = VisualTreeHelper.GetParent(source);
                if (visualParent != null) parents.Add(visualParent);
            }
            foreach(var parent in parents)
            {
                if (parent is T) yield return parent as T;
                foreach (var next in parent.Parents<T>()) yield return next;
            }
        }

        public static IEnumerable<T> Children<T>(this DependencyObject source) where T: DependencyObject
        {
            if (source == null) yield break;
            HashSet<DependencyObject> children = new HashSet<DependencyObject>();
            var logicChildren = LogicalTreeHelper.GetChildren(source);
            if(logicChildren != null)
            {
                foreach(var child in logicChildren)
                {
                    if (child is DependencyObject) children.Add(child as DependencyObject);
                }
            }
            if(source is Visual || source is Visual3D)
            {
                int childrenCount = VisualTreeHelper.GetChildrenCount(source);
                for (int i = 0; i < childrenCount; i++)
                {
                    var child = VisualTreeHelper.GetChild(source, i);
                    if (child != null) children.Add(child);
                }
            }
            foreach(var child in children)
            {
                if (child is T) yield return child as T;
                foreach (var next in child.Children<T>()) yield return next;
            }
        }
    }
}
