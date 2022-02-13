using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Editor.Abstractions;
using MarsOffice.Tvg.Notifications.Abstractions;
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
    public class StitchVideoResponseConsumer
    {
        private readonly IConfiguration _config;
        private readonly IMapper _mapper;

        public StitchVideoResponseConsumer(IConfiguration config, IMapper mapper)
        {
            _config = config;
            _mapper = mapper;
        }

        [FunctionName("StitchVideoResponseConsumer")]
        public async Task Run(
            [QueueTrigger("stitch-video-response", Connection = "localsaconnectionstring")] StitchVideoResponse response,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            // [Queue("notifications", Connection = "localsaconnectionstring")] IAsyncCollector<RequestNotification> notificationsQueue,
            // TODO upload
            ILogger log)
        {
            var dto = new Video
            {
                Id = response.VideoId,
                JobId = response.JobId,
                UserEmail = response.UserEmail,
                UserId = response.UserId,
                Error = response.Error,
                Status = response.Success ? VideoStatus.Generating : VideoStatus.Error
            };
            try
            {
                if (!response.Success)
                {
                    var updateOp = TableOperation.Merge(new VideoEntity
                    {
                        Error = response.Error,
                        PartitionKey = response.JobId,
                        RowKey = response.VideoId,
                        Status = (int)VideoStatus.Error,
                        ETag = "*"
                    });
                    await videosTable.ExecuteAsync(updateOp);

                    dto.Status = VideoStatus.Error;
                    dto.Error = response.Error;
                }
                else
                {
                    var query = new TableQuery<VideoEntity>()
                        .Where(
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, response.JobId),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, response.VideoId)
                                )
                        )
                        .Take(1);
                    var existingEntity = (await videosTable.ExecuteQuerySegmentedAsync<VideoEntity>(query, null)).Results.FirstOrDefault();
                    _mapper.Map(existingEntity, dto);
                    if (existingEntity.Status == (int)VideoStatus.Error || existingEntity.Status == (int)VideoStatus.Generated || existingEntity.Status == (int)VideoStatus.Uploaded)
                    {
                        return;
                    }

                    var mergeEntity = new VideoEntity
                    {
                        PartitionKey = response.JobId,
                        RowKey = response.VideoId,
                        UpdatedDate = DateTimeOffset.UtcNow,
                        ETag = "*",
                        FinalFile = response.FinalVideoLink,
                        FinalFileSasUrl = response.SasUrl,
                        Status = (int)VideoStatus.Generated
                    };

                    var mergeOp = TableOperation.Merge(mergeEntity);
                    await videosTable.ExecuteAsync(mergeOp);
                    _mapper.Map(mergeEntity, existingEntity);
                    _mapper.Map(existingEntity, dto);

                    if (existingEntity.DisabledAutoUpload == true)
                    {
                        //// notif
                        //await notificationsQueue.AddAsync(new RequestNotification { 
                        //    NotificationTypes = new [] {NotificationType.InApp, NotificationType.Email},
                        //    PlaceholderData = new System.Collections.Generic.Dictionary<string, string> {
                        //        {"name", existingEntity.Name }
                        //    },
                        //    Recipients = new []
                        //    {
                        //        new Recipient
                        //        {
                        //            Email = response.UserEmail,
                        //            UserId = response.UserId
                        //        }
                        //    },
                        //    Severity = Severity.Success,
                        //    TemplateName = "VideoGenerated"
                        //});
                        //await notificationsQueue.FlushAsync();

                    } else
                    {
                        // TODO queue tiktok
                    }

                    try
                    {
                        using var serviceManager = new ServiceManagerBuilder()
                            .WithOptions(option =>
                            {
                                option.ConnectionString = _config["signalrconnectionstring"];
                            })
                            .BuildServiceManager();
                        using var hubContext = await serviceManager.CreateHubContextAsync("main", CancellationToken.None);
                        await hubContext.Clients.User(response.UserId).SendAsync("videoUpdate", dto, CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "SignalR sending error");
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError(e, "Function threw an exception");

                var updateOp = TableOperation.Merge(new VideoEntity
                {
                    Error = e.Message,
                    PartitionKey = response.JobId,
                    RowKey = response.VideoId,
                    Status = (int)VideoStatus.Error,
                    ETag = "*"
                });
                await videosTable.ExecuteAsync(updateOp);

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
                    await hubContext.Clients.User(response.UserId).SendAsync("videoUpdate", dto, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "SignalR sending error");
                }
            }
        }
    }
}
