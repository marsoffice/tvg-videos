using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Speech.Abstractions;
using MarsOffice.Tvg.Translate.Abstractions;
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
    public class TranslationResponseConsumer
    {
        private readonly IConfiguration _config;
        private readonly IMapper _mapper;

        public TranslationResponseConsumer(IConfiguration config, IMapper mapper)
        {
            _config = config;
            _mapper = mapper;
        }

        [FunctionName("TranslateResponseConsumer")]
        public async Task Run(
            [QueueTrigger("translation-response", Connection = "localsaconnectionstring")] TranslationResponse response,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
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
                        ContentText = JsonConvert.SerializeObject(response.TranslatedSentences, new JsonSerializerSettings
                        {
                            ContractResolver = new CamelCasePropertyNamesContractResolver()
                        }),
                        TranslationDone = true
                    };

                    var mergeOp = TableOperation.Merge(mergeEntity);
                    await videosTable.ExecuteAsync(mergeOp);
                    _mapper.Map(mergeEntity, existingEntity);
                    _mapper.Map(existingEntity, dto);


                    await requestSpeechQueue.AddAsync(new RequestSpeech
                    {
                        VideoId = response.VideoId,
                        UserId = response.UserId,
                        UserEmail = response.UserEmail,
                        JobId = response.JobId,
                        Sentences = response.TranslatedSentences,
                        SpeechLanguage = existingEntity.SpeechLanguage,
                        SpeechPauseAfterInMillis = existingEntity.SpeechPauseAfterInMillis,
                        SpeechPauseBeforeInMillis = existingEntity.SpeechPauseBeforeInMillis,
                        SpeechPitch = existingEntity.SpeechPitch,
                        SpeechSpeed = existingEntity.SpeechSpeed,
                        SpeechType = existingEntity.SpeechType
                    });
                    await requestSpeechQueue.FlushAsync();

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
