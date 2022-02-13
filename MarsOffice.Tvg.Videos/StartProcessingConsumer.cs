using System;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Content.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class StartProcessingConsumer
    {
        private readonly IMapper _mapper;
        private readonly IConfiguration _config;

        public StartProcessingConsumer(IMapper mapper, IConfiguration configuration)
        {
            _mapper = mapper;
            _config = configuration;
        }

        [FunctionName("StartProcessingConsumer")]
        public async Task Run([QueueTrigger("start-processing", Connection = "localsaconnectionstring")] StartProcessing request,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            [Queue("request-content", Connection = "localsaconnectionstring")] IAsyncCollector<RequestContent> requestContentQueue,
            ILogger log)
        {
            try
            {
                var mergeOperation = TableOperation.Merge(new VideoEntity
                {
                    PartitionKey = request.Video.JobId,
                    RowKey = request.Video.Id,
                    ETag = "*",
                    Status = VideoStatus.Generating,
                });
                await videosTable.ExecuteAsync(mergeOperation);

                var dto = request.Video;
                dto.Status = VideoStatus.Generating;

                try
                {
                    using var serviceManager = new ServiceManagerBuilder()
                        .WithOptions(option =>
                        {
                            option.ConnectionString = _config["signalrconnectionstring"];
                        })
                        .BuildServiceManager();
                    using var hubContext = await serviceManager.CreateHubContextAsync("main", CancellationToken.None);
                    await hubContext.Clients.User(request.Job.UserId).SendAsync("videoUpdate", dto, CancellationToken.None);
                }
                catch (Exception e)
                {
                    log.LogError(e, "SignalR sending error");
                }

                // 1. Fire request content
                await requestContentQueue.AddAsync(new RequestContent { 
                    ContentGetLatestPosts = request.Video.ContentGetLatestPosts,
                    ContentIncludeLinks = request.Video.ContentIncludeLinks,
                    ContentMaxChars = request.Video.ContentMaxChars,
                    ContentMaxPosts = request.Video.ContentMaxPosts,
                    ContentMinChars = request.Video.ContentMinChars,
                    ContentMinPosts = request.Video.ContentMinPosts,
                    ContentNoOfIncludedTopComments = request.Video.ContentNoOfIncludedTopComments,
                    ContentStartDate = request.Video.ContentStartDate,
                    ContentTopic = request.Video.ContentTopic,
                    ContentTranslateFromLanguage = request.Video.ContentTranslateFromLanguage,
                    ContentTranslateToLanguage = request.Video.ContentTranslateToLanguage,
                    ContentType = request.Video.ContentType,
                    JobId = request.Job.Id,
                    UserEmail = request.Video.UserEmail,
                    UserId = request.Video.UserId,
                    VideoId = request.Video.Id
                });
                await requestContentQueue.FlushAsync();
            }
            catch (Exception e)
            {
                log.LogError(e, "Function threw an exception");
                var mergeOperation = TableOperation.Merge(new VideoEntity
                {
                    PartitionKey = request.Video.JobId,
                    RowKey = request.Video.Id,
                    Error = e.Message,
                    ETag = "*",
                    Status = VideoStatus.Error
                });
                await videosTable.ExecuteAsync(mergeOperation);

                var dto = request.Video;
                dto.Status = VideoStatus.Error;
                dto.Error = e.Message;

                try
                {
                    using var serviceManager = new ServiceManagerBuilder()
                        .WithOptions(option =>
                        {
                            option.ConnectionString = _config["signalrconnectionstring"];
                        })
                        .BuildServiceManager();
                    using var hubContext = await serviceManager.CreateHubContextAsync("main", CancellationToken.None);
                    await hubContext.Clients.User(request.Job.UserId).SendAsync("videoUpdate", dto, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "SignalR sending error");
                }
            }
        }
    }
}
