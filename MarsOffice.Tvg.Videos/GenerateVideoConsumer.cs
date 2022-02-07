using System;
using MarsOffice.Tvg.Videos.Abstractions;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class GenerateVideoConsumer
    {
        public GenerateVideoConsumer()
        {

        }

        [FunctionName("ConsumeGenerateVideo")]
        public void ConsumeGenerateVideo([QueueTrigger("generate-video", Connection = "localsaconnectionstring")] GenerateVideo myQueueItem,
        ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
        }
    }
}
