using System;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class GenerateVideoConsumer
    {
        private readonly IMapper _mapper;
        private readonly IConfiguration _config;

        public GenerateVideoConsumer(IMapper mapper, IConfiguration config)
        {
            _mapper = mapper;
            _config = config;
        }

        [FunctionName("GenerateVideoConsumer")]
        public async Task Run([QueueTrigger("generate-video", Connection = "localsaconnectionstring")] CloudQueueMessage message,
        [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
        [Queue("start-processing", Connection = "localsaconnectionstring")] IAsyncCollector<StartProcessing> startProcessingQueue,
        ILogger log)
        {
            var request = Newtonsoft.Json.JsonConvert.DeserializeObject<GenerateVideo>(message.AsString,
                    new Newtonsoft.Json.JsonSerializerSettings
                    {
                        ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver(),
                        NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore
                    });
            try
            {
                var dtoTemp = _mapper.Map<Video>(request.Job);
                var newVideo = _mapper.Map<VideoEntity>(dtoTemp);
                newVideo.Id = Guid.NewGuid().ToString();
                newVideo.JobId = request.Job.Id;
                newVideo.Name = request.Job.Name;
                newVideo.ETag = "*";
                newVideo.Status = (int)VideoStatus.Created;
                newVideo.CreatedDate = DateTimeOffset.UtcNow;
                newVideo.JobFireDate = request.RequestDate;
                newVideo.UserId = request.Job.UserId;
                newVideo.UserEmail = request.Job.UserEmail;
                newVideo.PartitionKey = newVideo.JobId;
                newVideo.RowKey = newVideo.Id;
                newVideo.CreateDone = true;

                if (!string.IsNullOrEmpty(newVideo.SelectedContent))
                {
                    newVideo.ContentText = newVideo.SelectedContent;
                    newVideo.ContentDone = true;
                }

                if (!string.IsNullOrEmpty(newVideo.SelectedAudioBackground))
                {
                    newVideo.AudioBackgroundDone = true;
                    newVideo.AudioBackgroundFile = "audio/" + newVideo.SelectedAudioBackground;
                }

                if (!string.IsNullOrEmpty(newVideo.SelectedVideoBackground))
                {
                    newVideo.VideoBackgroundDone = true;
                    newVideo.VideoBackgroundFile = "video/" + newVideo.SelectedVideoBackground;
                }
                
                var insertOperation = TableOperation.Insert(newVideo);
                await videosTable.ExecuteAsync(insertOperation);
                var dto = _mapper.Map<Video>(newVideo);

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

                await startProcessingQueue.AddAsync(new StartProcessing
                {
                    Job = request.Job,
                    Video = dto
                });
                await startProcessingQueue.FlushAsync();
            }
            catch (Exception e)
            {
                log.LogError(e, "Function threw an exception");
                if (message.DequeueCount >= 5)
                {
                    var videoEntity = new VideoEntity
                    {
                        Id = Guid.NewGuid().ToString(),
                        JobId = request.Job.Id,
                        ETag = "*",
                        Status = (int)VideoStatus.Error,
                        Error = e.Message,
                        CreatedDate = DateTimeOffset.UtcNow,
                        JobFireDate = request.RequestDate,
                        UserId = request.Job.UserId,
                        UserEmail = request.Job.UserEmail
                    };
                    videoEntity.PartitionKey = videoEntity.JobId;
                    videoEntity.RowKey = videoEntity.Id;

                    var insertOperation = TableOperation.InsertOrMerge(videoEntity);
                    await videosTable.ExecuteAsync(insertOperation);

                    try
                    {
                        using var serviceManager = new ServiceManagerBuilder()
                            .WithOptions(option =>
                            {
                                option.ConnectionString = _config["signalrconnectionstring"];
                            })
                            .BuildServiceManager();
                        var dto = _mapper.Map<Video>(videoEntity);
                        using var hubContext = await serviceManager.CreateHubContextAsync("main", CancellationToken.None);
                        await hubContext.Clients.User(request.Job.UserId).SendAsync("videoUpdate", dto, CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "SignalR sending error");
                    }
                }
                throw;
            }
        }
    }
}
