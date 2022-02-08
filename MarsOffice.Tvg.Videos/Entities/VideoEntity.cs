using System;
using MarsOffice.Tvg.Videos.Abstractions;
using Microsoft.Azure.Cosmos.Table;

namespace MarsOffice.Tvg.Videos.Entities
{
    public class VideoEntity : TableEntity
    {
        public string Id { get; set; }
        public string JobId { get; set; }
        public string UserId { get; set; }
        public string UserEmail { get; set; }
        public DateTimeOffset JobFireDate { get; set; }
        public DateTimeOffset CreatedDate { get; set; }
        public DateTimeOffset? UpdatedDate { get; set; }
        public VideoStatus Status { get; set; }
        public string Error { get; set; }
        public string ContentText { get; set; }
        public string SpeechFile { get; set; }
        public string AudioBackgroundFile { get; set; }
        public string VideoBackgroundFile { get; set; }
        public string EditorJobId { get; set; }
        public string FinalFile { get; set; }
        public long? FinalFileDurationInMillis { get; set; }
        public long? FinalFileSizeInBytes { get; set; }
    }
}