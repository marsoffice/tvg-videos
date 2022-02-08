using System;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class GenerateVideoConsumer
    {
        private readonly IMapper _mapper;
        public GenerateVideoConsumer(IMapper mapper)
        {
            _mapper = mapper;
        }

        [FunctionName("GenerateVideoConsumer")]
        public async Task Run([QueueTrigger("generate-video", Connection = "localsaconnectionstring")] GenerateVideo request,
        [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
        [Queue("start-processing", Connection = "localsaconnectionstring")] IAsyncCollector<StartProcessing> startProcessingQueue,
        ILogger log)
        {
            var newVideo = new VideoEntity { 
                Id = Guid.NewGuid().ToString(),
                JobId = request.Job.Id,
                ETag = "*",
                Status = VideoStatus.Created,
                CreatedDate = DateTimeOffset.UtcNow,
                JobFireDate = request.RequestDate,
                UserId = request.Job.UserId,
                UserEmail = request.Job.UserEmail
            };
            newVideo.PartitionKey = newVideo.JobId;
            newVideo.RowKey = newVideo.Id;

            var insertOperation = TableOperation.InsertOrReplace(newVideo);
            await videosTable.ExecuteAsync(insertOperation);

            await startProcessingQueue.AddAsync(new StartProcessing {
                Job = request.Job,
                Video = _mapper.Map<Video>(newVideo)
            });
            await startProcessingQueue.FlushAsync();
        }
    }
}
