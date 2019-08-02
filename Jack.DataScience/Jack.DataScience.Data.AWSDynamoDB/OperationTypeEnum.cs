using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AWSDynamoDB
{
    public enum OperationTypeEnum
    {
        None,
        Reboot,
        Stop,
        Terminate,
        LaunchMore,
        TerminateAndLaunchNew,
    }
}
