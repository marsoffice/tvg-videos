using System;
using System.Collections.Generic;
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
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.Storage.Queue.Protocol;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

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
        public async Task Run([QueueTrigger("start-processing", Connection = "localsaconnectionstring")] CloudQueueMessage message,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            [Queue("request-content", Connection = "localsaconnectionstring")] IAsyncCollector<RequestContent> requestContentQueue,
            [Queue("request-videobackground", Connection = "localsaconnectionstring")] IAsyncCollector<RequestVideoBackground> requestVideoBackgroundQueue,
            [Queue("request-audiobackground", Connection = "localsaconnectionstring")] IAsyncCollector<RequestAudioBackground> requestAudioBackgroundQueue,
            [Queue("request-translation", Connection = "localsaconnectionstring")] IAsyncCollector<RequestTranslation> requestTranslationQueue,
            [Queue("request-speech", Connection = "localsaconnectionstring")] IAsyncCollector<RequestSpeech> requestSpeechQueue,
            ILogger log)
        {
            var request = Newtonsoft.Json.JsonConvert.DeserializeObject<StartProcessing>(message.AsString,
                    new Newtonsoft.Json.JsonSerializerSettings
                    {
                        ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver(),
                        NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore
                    });
            try
            {
                var mergeOperation = TableOperation.Merge(new VideoEntity
                {
                    PartitionKey = request.Video.JobId,
                    RowKey = request.Video.Id,
                    UpdatedDate = DateTimeOffset.UtcNow,
                    ETag = "*",
                    Status = (int)VideoStatus.Generating,
                });
                await videosTable.ExecuteAsync(mergeOperation);

                var dto = request.Video;
                dto.UpdatedDate = DateTimeOffset.UtcNow;
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
                if (request.Video.ContentDone != true)
                {
                    await requestContentQueue.AddAsync(new RequestContent
                    {
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
                else
                {
                    if (!string.IsNullOrEmpty(request.Video.ContentTranslateFromLanguage))
                    {
                        await requestTranslationQueue.AddAsync(new RequestTranslation
                        {
                            FromLangCode = request.Video.ContentTranslateFromLanguage,
                            ToLangCode = request.Video.ContentTranslateToLanguage,
                            Sentences = JsonConvert.DeserializeObject<IEnumerable<string>>(request.Video.ContentText, new JsonSerializerSettings {
                                ContractResolver = new CamelCasePropertyNamesContractResolver()
                            }),
                            JobId = request.Video.JobId,
                            UserId = request.Video.UserId,
                            VideoId = request.Video.Id,
                            UserEmail = request.Video.UserEmail,
                        });
                        await requestTranslationQueue.FlushAsync();
                    }
                    else
                    {
                        await requestSpeechQueue.AddAsync(new RequestSpeech
                        {
                            VideoId = request.Video.Id,
                            UserId = request.Video.UserId,
                            UserEmail = request.Video.UserEmail,
                            JobId = request.Video.JobId,
                            Sentences = JsonConvert.DeserializeObject<IEnumerable<string>>(request.Video.ContentText, new JsonSerializerSettings
                            {
                                ContractResolver = new CamelCasePropertyNamesContractResolver()
                            }),
                            SpeechLanguage = request.Video.SpeechLanguage,
                            SpeechPauseAfterInMillis = request.Video.SpeechPauseAfterInMillis,
                            SpeechPauseBeforeInMillis = request.Video.SpeechPauseBeforeInMillis,
                            SpeechPitch = request.Video.SpeechPitch,
                            SpeechSpeed = request.Video.SpeechSpeed,
                            SpeechType = request.Video.SpeechType
                        });
                        await requestSpeechQueue.FlushAsync();
                    }

                    if (request.Video.AudioBackgroundDone != true)
                    {
                        await requestAudioBackgroundQueue.AddAsync(new RequestAudioBackground
                        {
                            Category = "unknown",
                            VideoId = request.Video.Id,
                            JobId = request.Video.JobId,
                            LanguageCode = request.Video.SpeechLanguage,
                            UserEmail = request.Video.UserEmail,
                            UserId = request.Video.UserId
                        });
                        await requestAudioBackgroundQueue.FlushAsync();
                    }

                    if (request.Video.VideoBackgroundDone != true)
                    {
                        await requestVideoBackgroundQueue.AddAsync(new RequestVideoBackground
                        {
                            Category = "unknown",
                            VideoId = request.Video.Id,
                            JobId = request.Video.JobId,
                            LanguageCode = request.Video.SpeechLanguage,
                            UserEmail = request.Video.UserEmail,
                            UserId = request.Video.UserId
                        });
                        await requestVideoBackgroundQueue.FlushAsync();
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError(e, "Function threw an exception");
                if (message.DequeueCount >= 5)
                {
                    var mergeOperation = TableOperation.Merge(new VideoEntity
                    {
                        PartitionKey = request.Video.JobId,
                        RowKey = request.Video.Id,
                        Error = e.Message,
                        ETag = "*",
                        Status = (int)VideoStatus.Error
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
                throw;
            }
        }
    }
}
