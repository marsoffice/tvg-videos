using System;
using System.Threading.Tasks;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class StartProcessingConsumer
    {
        [FunctionName("StartProcessingConsumer")]
        public async Task Run([QueueTrigger("start-processing", Connection = "localsaconnectionstring")] StartProcessing request,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            ILogger log)
        {
            var mergeOperation = TableOperation.Merge(new VideoEntity {
                PartitionKey = request.Video.JobId,
                RowKey = request.Video.Id,
                ETag = "*",
                Status = VideoStatus.Generating
            });
            await videosTable.ExecuteAsync(mergeOperation);

            // TODO
        }
    }
}
