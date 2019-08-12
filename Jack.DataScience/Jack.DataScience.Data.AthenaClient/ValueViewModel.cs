using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Text;

namespace Jack.DataScience.Data.AthenaClient
{
    public abstract class ValueViewModel
    {
        public abstract void OnValueChange(string propertyName);
    }


    public class ValueViewModel<T>: ValueViewModel, INotifyPropertyChanged
    {
        private readonly QueryParameter queryParameter;
        private readonly Type type;
        public ValueViewModel(QueryParameter queryParameter)
        {
            this.queryParameter = queryParameter;
            type = typeof(T);
        }

        private T _Value;
        public T Value
        {
            get
            {
                if (type == typeof(string))
                {
                    return (T)(object)(queryParameter.Value);
                }
                else if (type == typeof(long))
                {
                    long result = 0L;
                    long.TryParse(queryParameter.Value, out result);
                    return (T)(object)result;
                }
                else if(type == typeof(double))
                {
                    double result = 0D;
                    double.TryParse(queryParameter.Value, out result);
                    return (T)(object)result;
                }
                else if(type == typeof(bool))
                {
                    bool result = false;
                    bool.TryParse(queryParameter.Value, out result);
                    return (T)(object)result;
                }
                return default(T);
            }
            set
            {
                string strValue = value.ToString();
                if (type == typeof(bool)) strValue = strValue.ToLower();
                if (queryParameter.Value != strValue)
                {
                    queryParameter.Value = strValue;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
                    queryParameter.OnValueChange(nameof(queryParameter.Value));
                }
            }
        }

        private List<string> _Values;
        public List<string> Values
        {
            get
            {
                return queryParameter.Values;
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public override void OnValueChange(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

    }
}
