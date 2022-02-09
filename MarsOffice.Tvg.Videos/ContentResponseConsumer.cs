using System;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Content.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class ContentResponseConsumer
    {
        private readonly IMapper _mapper;
        private readonly IConfiguration _config;

        public ContentResponseConsumer(IMapper mapper, IConfiguration config)
        {
            _mapper = mapper;
            _config = config;
        }

        [FunctionName("ContentResponseConsumer")]
        public async Task Run(
            [QueueTrigger("content-response", Connection = "localsaconnectionstring")]ContentResponse response,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            ILogger log)
        {
            try {
                // TODO
            } catch (Exception e) {
                log.LogError(e, "Function threw an exception");

                try
                {
                    var dto = new Video {
                        Id = response.VideoId,
                        JobId = response.JobId,
                        UserId = response.UserId,
                        Error = e.Message,
                        Status = VideoStatus.Error
                     };
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
