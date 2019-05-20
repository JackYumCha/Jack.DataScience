using Autofac;

namespace Jack.DataScience.Compute.AWSBatch
{
    public class AWSBatchModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSBatchAPI>();
            base.Load(builder);
        }
    }
}
