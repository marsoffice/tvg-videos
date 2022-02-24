using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Microfunction;
using MarsOffice.Tvg.TikTok.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class Videos
    {
        private readonly IMapper _mapper;
        private readonly IConfiguration _config;

        public Videos(IMapper mapper, IConfiguration config)
        {
            _mapper = mapper;
            _config = config;
        }

        [FunctionName("GetJobVideos")]
        public async Task<IActionResult> GetJobVideos(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "api/videos/getJobVideos/{id}")] HttpRequest req,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            ILogger log)
        {
            try
            {
                var principal = MarsOfficePrincipal.Parse(req);
                var uid = principal.FindFirst("id").Value;
                var jobId = req.RouteValues["id"].ToString();

                var entities = new List<VideoEntity>();

                var query = new TableQuery<VideoEntity>()
                    .Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, jobId),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("UserId", QueryComparisons.Equal, uid)
                        )
                    )
                    .OrderByDesc("Timestamp");
                var hasData = true;
                TableContinuationToken tct = null;
                while (hasData)
                {
                    var response = await videosTable.ExecuteQuerySegmentedAsync(query, tct);
                    entities.AddRange(response);
                    tct = response.ContinuationToken;
                    if (tct == null)
                    {
                        hasData = false;
                    }
                }
                return new OkObjectResult(
                   _mapper.Map<IEnumerable<Video>>(entities)
                    );
            }
            catch (Exception e)
            {
                log.LogError(e, "Exception occured in function");
                return new BadRequestObjectResult(Errors.Extract(e));
            }
        }

        [FunctionName("DeleteVideo")]
        public async Task<IActionResult> DeleteVideo(
            [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "api/videos/delete/{jobId}/{id}")] HttpRequest req,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            ILogger log)
        {
            try
            {
                var principal = MarsOfficePrincipal.Parse(req);
                var uid = principal.FindFirst("id").Value;
                var jobId = req.RouteValues["jobId"].ToString();
                var id = req.RouteValues["id"].ToString();
                var delOp = TableOperation.Delete(new VideoEntity
                {
                    PartitionKey = jobId,
                    RowKey = id,
                    UserId = uid,
                    ETag = "*"
                });
                await videosTable.ExecuteAsync(delOp);

                var csa = Microsoft.Azure.Storage.CloudStorageAccount.Parse(_config["localsaconnectionstring"]);
                var blobClient = csa.CreateCloudBlobClient();
                var containerReference = blobClient.GetContainerReference("editor");
                var blobRef = containerReference.GetBlockBlobReference($"{id}.mp4");
                await blobRef.DeleteIfExistsAsync();

                var jobsDataContainerReference = blobClient.GetContainerReference("jobsdata");
                var ttsBlobRef = jobsDataContainerReference.GetBlockBlobReference($"{id}/tts.mp3");
                await ttsBlobRef.DeleteIfExistsAsync();
                return new OkResult();
            }
            catch (Exception e)
            {
                log.LogError(e, "Exception occured in function");
                return new BadRequestObjectResult(Errors.Extract(e));
            }
        }

        [FunctionName("UploadVideo")]
        public async Task<IActionResult> UploadVideo(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "api/videos/upload/{jobId}/{id}")] HttpRequest req,
            [Table("Videos", Connection = "localsaconnectionstring")] CloudTable videosTable,
            [Queue("request-upload-video", Connection = "localsaconnectionstring")] IAsyncCollector<RequestUploadVideo> requestUploadVideoQueue,
            ILogger log)
        {
            try
            {
                var principal = MarsOfficePrincipal.Parse(req);
                var uid = principal.FindFirst("id").Value;
                var jobId = req.RouteValues["jobId"].ToString();
                var id = req.RouteValues["id"].ToString();
                var query = new TableQuery<VideoEntity>()
                    .Where(
                        TableQuery.CombineFilters(
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, jobId),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, id)
                            ),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("UserId", QueryComparisons.Equal, uid)
                        )
                    )
                    .Take(1);

                var entity = (await videosTable.ExecuteQuerySegmentedAsync(query, null)).Results.First();

                if (entity.AutoUploadTikTokAccounts == null || !entity.AutoUploadTikTokAccounts.Any())
                {
                    throw new Exception("No TikTok accounts set for upload");
                }

                if (string.IsNullOrEmpty(entity.FinalFile))
                {
                    throw new Exception("Video not generated yet.");
                }

                await requestUploadVideoQueue.AddAsync(new RequestUploadVideo
                {
                    JobId = jobId,
                    VideoId = id,
                    UserId = uid,
                    VideoPath = entity.FinalFile.Replace("/devstoreaccount1/", ""),
                    OpenIds = entity.AutoUploadTikTokAccounts?.Split(",")
                });
                await requestUploadVideoQueue.FlushAsync();

                entity.Status = (int)VideoStatus.Uploading;
                entity.ETag = "*";
                var updateOp = TableOperation.Merge(entity);
                await videosTable.ExecuteAsync(updateOp);

                var dto = _mapper.Map<Video>(entity);
                try
                {
                    using var serviceManager = new ServiceManagerBuilder()
                        .WithOptions(option =>
                        {
                            option.ConnectionString = _config["signalrconnectionstring"];
                        })
                        .BuildServiceManager();
                    using var hubContext = await serviceManager.CreateHubContextAsync("main", CancellationToken.None);
                    await hubContext.Clients.User(uid).SendAsync("videoUpdate", dto, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "SignalR sending error");
                }

                return new OkResult();
            }
            catch (Exception e)
            {
                log.LogError(e, "Exception occured in function");
                return new BadRequestObjectResult(Errors.Extract(e));
            }
        }
    }
}
