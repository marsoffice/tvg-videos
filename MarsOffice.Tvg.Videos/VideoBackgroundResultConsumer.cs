using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Editor.Abstractions;
using MarsOffice.Tvg.VideoDownloader.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.Storage.Queue.Protocol;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace MarsOffice.Tvg.Videos
{
    public class VideoBackgroundResultConsumer
    {
        private readonly IConfiguration _config;
        private readonly IMapper _mapper;

        public VideoBackgroundResultConsumer(IConfiguration config, IMapper mapper)
        {
            _config = config;
            _mapper = mapper;
        }

        [FunctionName("VideoBackgroundResultConsumer")]
        public async Task Run(
            [QueueTrigger("videobackground-result", Connection = "localsaconnectionstring")] QueueMessage message,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            [Queue("request-stitch-video", Connection = "localsaconnectionstring")] IAsyncCollector<RequestStitchVideo> requestStitchVideoQueue,
            ILogger log)
        {
            var response = Newtonsoft.Json.JsonConvert.DeserializeObject<VideoBackgroundResult>(message.Text,
                    new Newtonsoft.Json.JsonSerializerSettings
                    {
                        ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver(),
                        NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore
                    });
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
                        VideoBackgroundDone = true,
                        VideoBackgroundFile = response.FileLink
                    };

                    var mergeOp = TableOperation.Merge(mergeEntity);
                    await videosTable.ExecuteAsync(mergeOp);
                    _mapper.Map(mergeEntity, existingEntity);
                    _mapper.Map(existingEntity, dto);

                    if (
                            existingEntity.ContentDone == true &&
                            existingEntity.SpeechDone == true &&
                            existingEntity.AudioBackgroundDone == true &&
                            existingEntity.VideoBackgroundDone == true &&
                            existingEntity.CreateDone == true &&
                            (string.IsNullOrEmpty(existingEntity.ContentTranslateFromLanguage) || existingEntity.TranslationDone == true)

                     )
                    {
                        await requestStitchVideoQueue.AddAsync(new RequestStitchVideo { 
                            AudioBackgroundFileLink = existingEntity.AudioBackgroundFile,
                            AudioBackgroundVolumeInPercent = existingEntity.AudioBackgroundVolumeInPercent,
                            Durations = existingEntity.Durations.Split(",").Select(x => long.Parse(x)).ToList(),
                            JobId = response.JobId,
                            Resolution = existingEntity.EditorVideoResolution,
                            Sentences = JsonConvert.DeserializeObject<IEnumerable<string>>(existingEntity.ContentText, new JsonSerializerSettings {
                                ContractResolver = new CamelCasePropertyNamesContractResolver()
                            }),
                            TextBoxBorderColor = existingEntity.TextBoxBorderColor,
                            TextBoxColor = existingEntity.TextBoxColor,
                            TextBoxOpacity = existingEntity.TextBoxOpacity,
                            TextColor = existingEntity.TextColor,
                            TextFontFamily = existingEntity.TextFontFamily,
                            TextFontSize = existingEntity.TextFontSize,
                            UserEmail = response.UserEmail,
                            UserId = response.UserId,
                            VideoBackgroundFileLink = existingEntity.VideoBackgroundFile,
                            VideoId = response.VideoId,
                            VoiceFileLink = existingEntity.SpeechFile
                        });
                        await requestStitchVideoQueue.FlushAsync();
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
                if (message.DequeueCount >= 5)
                {
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
                throw;
            }
        }
    }
}
