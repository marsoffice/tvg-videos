using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.AudioDownloader.Abstractions;
using MarsOffice.Tvg.Content.Abstractions;
using MarsOffice.Tvg.Speech.Abstractions;
using MarsOffice.Tvg.Translate.Abstractions;
using MarsOffice.Tvg.VideoDownloader.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace MarsOffice.Tvg.Videos
{
    public class ContentResponseConsumer
    {
        private readonly IConfiguration _config;
        private readonly IMapper _mapper;

        public ContentResponseConsumer(IConfiguration config, IMapper mapper)
        {
            _config = config;
            _mapper = mapper;
        }

        [FunctionName("ContentReceivedConsumer")]
        public async Task Run(
            [QueueTrigger("content-response", Connection = "localsaconnectionstring")] ContentResponse response,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            [Queue("request-videobackground", Connection = "localsaconnectionstring")] IAsyncCollector<RequestVideoBackground> requestVideoBackgroundQueue,
            [Queue("request-audiobackground", Connection = "localsaconnectionstring")] IAsyncCollector<RequestAudioBackground> requestAudioBackgroundQueue,
            [Queue("request-translation", Connection = "localsaconnectionstring")] IAsyncCollector<RequestTranslation> requestTranslationQueue,
            [Queue("request-speech", Connection = "localsaconnectionstring")] IAsyncCollector<RequestSpeech> requestSpeechQueue,
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
                        Status = VideoStatus.Error,
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
                    if (existingEntity.Status == VideoStatus.Error || existingEntity.Status == VideoStatus.Generated || existingEntity.Status == VideoStatus.Uploaded 
                        || existingEntity.ContentDone == true)
                    {
                        return;
                    }

                    var mergeEntity = new VideoEntity
                    {
                        PartitionKey = response.JobId,
                        RowKey = response.VideoId,
                        UpdatedDate = DateTimeOffset.UtcNow,
                        ETag = "*",
                        ContentText = JsonConvert.SerializeObject(response.Posts.Select(x => x.Text).ToList(), new JsonSerializerSettings
                        {
                            ContractResolver = new CamelCasePropertyNamesContractResolver()
                        }),
                        ContentDone = true,
                        ContentCategory = response.Category,
                    };

                    var mergeOp = TableOperation.Merge(mergeEntity);
                    await videosTable.ExecuteAsync(mergeOp);
                    _mapper.Map(mergeEntity, existingEntity);
                    _mapper.Map(existingEntity, dto);


                    if (!string.IsNullOrEmpty(existingEntity.ContentTranslateFromLanguage))
                    {
                        await requestTranslationQueue.AddAsync(new RequestTranslation { 
                            FromLangCode = existingEntity.ContentTranslateFromLanguage,
                            ToLangCode = existingEntity.ContentTranslateToLanguage,
                            Sentences = response.Posts.Select(x => x.Text).ToList(),
                            JobId = response.JobId,
                            UserId = response.UserId,
                            VideoId = response.VideoId,
                            UserEmail = response.UserEmail,
                        });
                        await requestTranslationQueue.FlushAsync();
                    } else
                    {
                        await requestSpeechQueue.AddAsync(new RequestSpeech
                        {
                            VideoId = response.VideoId,
                            UserId = response.UserId,
                            UserEmail = response.UserEmail,
                            JobId = response.JobId,
                            Sentences = response.Posts.Select(x => x.Text).ToList(),
                            SpeechLanguage = existingEntity.SpeechLanguage,
                            SpeechPauseAfterInMillis = existingEntity.SpeechPauseAfterInMillis,
                            SpeechPauseBeforeInMillis = existingEntity.SpeechPauseBeforeInMillis,
                            SpeechPitch = existingEntity.SpeechPitch,
                            SpeechSpeed = existingEntity.SpeechSpeed,
                            SpeechType = existingEntity.SpeechType
                        });
                        await requestSpeechQueue.FlushAsync();
                    }

                    await requestAudioBackgroundQueue.AddAsync(new RequestAudioBackground { 
                        Category = response.Category,
                        VideoId = response.VideoId,
                        JobId = response.JobId,
                        LanguageCode = existingEntity.SpeechLanguage,
                        UserEmail = response.UserEmail,
                        UserId = response.UserId
                    });
                    await requestAudioBackgroundQueue.FlushAsync();

                    await requestVideoBackgroundQueue.AddAsync(new RequestVideoBackground
                    {
                        Category = response.Category,
                        VideoId = response.VideoId,
                        JobId = response.JobId,
                        LanguageCode = existingEntity.SpeechLanguage,
                        UserEmail = response.UserEmail,
                        UserId = response.UserId
                    });
                    await requestVideoBackgroundQueue.FlushAsync();
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
            catch (Exception e)
            {
                log.LogError(e, "Function threw an exception");

                var updateOp = TableOperation.Merge(new VideoEntity
                {
                    Error = e.Message,
                    PartitionKey = response.JobId,
                    RowKey = response.VideoId,
                    Status = VideoStatus.Error,
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
