using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel;
using System.Reflection;
using System.Linq;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.DataTypes
{
    public class TypeSchema
    {
        private static Dictionary<Type, TypeSchema> TypeSchemas = new Dictionary<Type, TypeSchema>();
        private static Identity identity = new Identity();
        public static TypeSchema GetSchema<T>() where T: class
        {
            Type type = typeof(T);
            return GetSchema(type);
        }

        public static TypeSchema GetSchema(Type type)
        {
            if (!TypeSchemas.ContainsKey(type))
            {
                var fields = type.GetProperties()
                    .Where(p => p.GetCustomAttribute<SchemaIgnoreAttribute>() == null)
                    .Select(p =>
                    {
                        TypeFieldSchema field = new TypeFieldSchema()
                        {
                            Name = p.Name,
                            FieldType = p.MapFieldType(),
                            IsIndex = p.GetCustomAttribute<IndexAttribute>() != null,
                            IsRequired = p.GetCustomAttribute<RequiredAttribute>() != null,
                            IsPrimaryKey = p.GetCustomAttribute<KeyAttribute>() != null,
                            OriginalPropertyType = p.PropertyType,
                            PropertyInfo = p,
                            IsNullable = p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() == TypeMapping.type_nullable
                        };
                        field.IsGeoType = (int)field.FieldType >= (int) FieldTypeEnum.GeometryPoint 
                            && (int) field.FieldType <= (int) FieldTypeEnum.GeometryCollection;
                        var stringLength = p.GetCustomAttribute<StringLengthAttribute>();
                        if (stringLength != null)
                        {
                            field.MaxLength = stringLength.MaximumLength;
                            field.MinLength = stringLength.MinimumLength;
                        }
                        var forceType = p.GetCustomAttribute<ForceTypeAttribute>();
                        if (forceType != null)
                        {
                            field.Converter = forceType.Converter;
                        }
                        else
                        {
                            field.Converter = identity;
                        }
                        return field;
                    });
                TypeSchema schema = new TypeSchema() {
                    Name = type.Name
                };
                foreach (var field in fields)
                {
                    schema.typeFields.Add(field.Name, field);
                }
                TypeSchemas.Add(type, schema);
            }
            return TypeSchemas[type];
        }

        public string Name { get; private set; }
        private Dictionary<string, TypeFieldSchema> typeFields = new Dictionary<string, TypeFieldSchema>();

        public IEnumerable<TypeFieldSchema> Fields
        {
            get => typeFields.Values;
        }

        public TypeFieldSchema GetField(string name)
        {
            if (!typeFields.ContainsKey(name))
                return null;
            return typeFields[name];
        }

        public string TableNameWithSuffix(string suffix)
        {
            return suffix == null ? Name : $"{Name}__{suffix}";
        }
    }
}
