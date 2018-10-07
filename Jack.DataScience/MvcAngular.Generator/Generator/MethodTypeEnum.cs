namespace MvcAngular.Generator
{
    /// <summary>
    /// Method type definitions for different type of method.
    /// The code generator will need the type of method to generate corresponding codes
    /// </summary>
    internal enum MethodTypeEnum
    {
        None,
        EmptyPost,
        EmptyGet,
        PlainPost,
        PlainGet,
        FileGet,
        MultiFilePost,
        /// <summary>
        /// Runtime exception, when get method has complex json object parameter
        /// </summary>
        IllegalGetParameter,
        ComplexPost,
        /// <summary>
        /// Runtime exception, when complex post method has no FromBody attribute in the parameter
        /// </summary>
        MultipleComplexError,
        /// <summary>
        /// Runtime exception, when complex post method has no FromBody attribute in the parameter
        /// </summary>
        NotFromBodyComplexError
    }
}