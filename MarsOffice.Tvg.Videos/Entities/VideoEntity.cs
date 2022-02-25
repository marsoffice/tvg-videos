using System;
using MarsOffice.Tvg.Videos.Abstractions;
using Microsoft.Azure.Cosmos.Table;

namespace MarsOffice.Tvg.Videos.Entities
{
    public class VideoEntity : TableEntity
    {
        public string Id { get; set; }
        public string JobId { get; set; }
        public string Name { get; set; }
        public string UserId { get; set; }
        public string UserEmail { get; set; }
        public DateTimeOffset? JobFireDate { get; set; }
        public DateTimeOffset? CreatedDate { get; set; }
        public DateTimeOffset? UpdatedDate { get; set; }
        public int? Status { get; set; }
        public string Error { get; set; }
        public string ContentText { get; set; }
        public string SelectedContent { get; set; }
        public string ContentCategory { get; set; }
        public string Durations { get; set; }
        public string SpeechFile { get; set; }
        public string AudioBackgroundFile { get; set; }
        public string VideoBackgroundFile { get; set; }
        public string FinalFile { get; set; }
        public string FinalFileSasUrl { get; set; }
        public long? FinalFileDurationInMillis { get; set; }
        public bool? TrimGracefullyToMaxDuration { get; set; }


        public bool? CreateDone { get; set; }
        public bool? ContentDone { get; set; }
        public bool? TranslationDone { get; set; }
        public bool? SpeechDone { get; set; }
        public bool? AudioBackgroundDone { get; set; }
        public bool? VideoBackgroundDone { get; set; }
        public bool? StitchDone { get; set; }
        public bool? UploadDone { get; set; }

        public int? PreferredDurationInSeconds { get; set; }
        public string ContentType { get; set; }
        public string ContentTopic { get; set; }
        public bool? ContentGetLatestPosts { get; set; }
        public DateTimeOffset? ContentStartDate { get; set; }
        public int? ContentMinChars { get; set; }
        public int? ContentMaxChars { get; set; }
        public string ContentTranslateFromLanguage { get; set; }
        public string ContentTranslateToLanguage { get; set; }
        public int? ContentNoOfIncludedTopComments { get; set; }
        public bool? ContentIncludeLinks { get; set; }
        public int? ContentMinPosts { get; set; }
        public int? ContentMaxPosts { get; set; }
        public int? SpeechPitch { get; set; }
        public int? SpeechSpeed { get; set; }
        public string SpeechType { get; set; }
        public string SpeechLanguage { get; set; }
        public long? SpeechPauseBeforeInMillis { get; set; }
        public long? SpeechPauseAfterInMillis { get; set; }
        public int? AudioBackgroundQuality { get; set; }
        public int? AudioBackgroundVolumeInPercent { get; set; }
        public string SelectedAudioBackground { get; set; }
        public string VideoBackgroundResolution { get; set; }
        public string SelectedVideoBackground { get; set; }
        public string TextFontFamily { get; set; }
        public int? TextFontSize { get; set; }
        public string TextBoxColor { get; set; }
        public string TextColor { get; set; }
        public int? TextBoxOpacity { get; set; }
        public string TextBoxBorderColor { get; set; }
        public bool? DisabledAutoUpload { get; set; }
        public string PostDescription { get; set; }
        public string EditorVideoResolution { get; set; }
        public string AutoUploadTikTokAccounts { get; set; }
    }
}