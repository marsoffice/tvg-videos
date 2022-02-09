using System;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
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
            ILogger log)
        {
            try
            {
                var mergeOperation = TableOperation.Merge(new VideoEntity
                {
                    PartitionKey = request.Video.JobId,
                    RowKey = request.Video.Id,
                    ETag = "*",
                    Status = VideoStatus.Generating
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

                // TODO logic

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
