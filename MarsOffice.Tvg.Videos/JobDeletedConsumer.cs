using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MarsOffice.Tvg.Jobs.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class JobDeletedConsumer
    {
        private readonly IConfiguration _config;

        public JobDeletedConsumer(IConfiguration config)
        {
            _config = config;
        }

        [FunctionName("JobDeletedConsumer")]
        public async Task Run([QueueTrigger("job-deleted", Connection = "localsaconnectionstring")] JobDeleted request,
        [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
        ILogger log)
        {
            try
            {
                var csa = Microsoft.Azure.Storage.CloudStorageAccount.Parse(_config["localsaconnectionstring"]);
                var blobClient = csa.CreateCloudBlobClient();
                var jobsDataContainer = blobClient.GetContainerReference("jobsdata");
                var editorContainer = blobClient.GetContainerReference("editor");

                var query = new TableQuery<VideoEntity>()
                    .Where(
                        TableQuery.GenerateFilterCondition(
                            "PartitionKey",
                            QueryComparisons.Equal,
                            request.JobId
                        )
                    );

                var deleteTasks = new List<Task<bool>>();
                var hasData = true;
                TableContinuationToken tct = null;
                while (hasData)
                {
                    var videos = await videosTable.ExecuteQuerySegmentedAsync(query, tct);
                    foreach (var video in videos)
                    {
                        var ttsBlob = jobsDataContainer.GetBlockBlobReference($"{video.RowKey}/tts.mp3");
                        deleteTasks.Add(ttsBlob.DeleteIfExistsAsync());

                        var finalBlob = editorContainer.GetBlockBlobReference($"{video.RowKey}.mp4");
                        deleteTasks.Add(finalBlob.DeleteIfExistsAsync());
                    }
                    tct = videos.ContinuationToken;
                    if (tct == null)
                    {
                        hasData = false;
                    }
                }
                var deleteOp = TableOperation.Delete(new VideoEntity
                {
                    PartitionKey = request.JobId,
                    ETag = "*"
                });
                await videosTable.ExecuteAsync(deleteOp);
                await Task.WhenAll(deleteTasks);
            }
            catch (Exception e)
            {
                log.LogError(e, "Function threw an error");
            }
        }
    }
}
