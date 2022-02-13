using AutoMapper;
using MarsOffice.Tvg.Videos.Abstractions;
using MarsOffice.Tvg.Videos.Entities;

namespace MarsOffice.Tvg.Videos.Mappers
{
    public class VideoMapper : Profile
    {
        public VideoMapper() {
            CreateMap<Video, VideoEntity>().PreserveReferences();
            CreateMap<VideoEntity, Video>().PreserveReferences();
            CreateMap<VideoEntity, VideoEntity>().PreserveReferences()
                .ForAllMembers(opt => opt.Condition((source, dest, sourceMember, destMember) => (sourceMember != null)));
        }
    }
}