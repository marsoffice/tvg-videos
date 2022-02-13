using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AutoMapper;
using MarsOffice.Microfunction;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace MarsOffice.Tvg.Videos
{
    public class Videos
    {
        private readonly IMapper _mapper;

        public Videos(IMapper mapper)
        {
            _mapper = mapper;
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
    }
}
