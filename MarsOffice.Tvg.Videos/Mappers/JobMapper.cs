using AutoMapper;
using MarsOffice.Tvg.Jobs.Abstractions;
using MarsOffice.Tvg.Videos.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarsOffice.Tvg.Videos.Mappers
{
    public class JobMapper : Profile
    {
        public JobMapper()
        {
            CreateMap<Job, Video>().PreserveReferences();
        }
    }
}
