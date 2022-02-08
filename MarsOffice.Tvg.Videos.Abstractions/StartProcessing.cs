using MarsOffice.Tvg.Jobs.Abstractions;

namespace MarsOffice.Tvg.Videos.Abstractions
{
    public class StartProcessing
    {
        public Job Job { get; set; }
        public Video Video { get; set; }
    }
}