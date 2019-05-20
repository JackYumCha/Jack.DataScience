using Autofac;

namespace Jack.DataScience.Http.AWSCloudFront
{
    public class AWSCloudFrontModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSCloudFrontAPI>();
        }
    }
}
