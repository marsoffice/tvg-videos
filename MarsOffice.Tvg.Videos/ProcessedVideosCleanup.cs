using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MarsOffice.Tvg.Jobs.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class ProcessedVideosCleanup
    {
        private readonly IConfiguration _config;

        public ProcessedVideosCleanup(IConfiguration config)
        {
            _config = config;
        }

        [FunctionName("ProcessedVideosCleanup")]
        public async Task Run([TimerTrigger("0 */30 * * * *")] TimerInfo myTimer,
        [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
        ILogger log)
        {
            try
            {
                var csa = Microsoft.Azure.Storage.CloudStorageAccount.Parse(_config["localsaconnectionstring"]);
                var blobClient = csa.CreateCloudBlobClient();
                var jobsDataContainer = blobClient.GetContainerReference("jobsdata");

                var nowMinusOneDay = DateTimeOffset.UtcNow.AddDays(-1);

                var query = new TableQuery<VideoEntity>()
                    .Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThanOrEqual, nowMinusOneDay),
                            TableOperators.Or,
                            TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition(
                                "Status",
                                QueryComparisons.Equal,
                                ((int)VideoStatus.Generated).ToString()
                            ),
                            TableOperators.Or,
                            TableQuery.GenerateFilterCondition(
                                "Status",
                                QueryComparisons.Equal,
                                ((int)VideoStatus.Uploaded).ToString()
                            )
                        )
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
                    }
                    tct = videos.ContinuationToken;
                    if (tct == null)
                    {
                        hasData = false;
                    }
                }
                await Task.WhenAll(deleteTasks);
            }
            catch (Exception e)
            {
                log.LogError(e, "Function threw an error");
            }
        }
    }
}
