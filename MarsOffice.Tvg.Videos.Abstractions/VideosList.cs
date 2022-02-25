using System.Collections.Generic;

namespace MarsOffice.Tvg.Videos.Abstractions
{
    public class VideosList
    {
        public string NextRowKey { get; set; }
        public IEnumerable<Video> Items { get; set; }
    }
}