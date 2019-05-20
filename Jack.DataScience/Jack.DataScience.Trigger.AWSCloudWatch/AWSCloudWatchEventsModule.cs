using Autofac;

namespace Jack.DataScience.Trigger.AWSCloudWatch
{
    public class AWSCloudWatchEventsModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSCloudWatchEventsAPI>();
        }
    }
}
