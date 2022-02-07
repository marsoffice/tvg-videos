using System;
using MarsOffice.Tvg.Jobs.Abstractions;

namespace MarsOffice.Tvg.Videos.Abstractions
{
    public class GenerateVideo
    {
        public DateTimeOffset RequestDate { get; set; }
        public Job Job { get; set; }
    }
}