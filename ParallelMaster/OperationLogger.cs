using Microsoft.Extensions.Logging;

namespace ParallelMaster;

internal class OperationLogger
{
    private readonly ILogger? _logger;
    public CountersEntity Counters;

    public OperationLogger(ILogger? logger)
    {
        _logger = logger;
        Counters = new();
    }

    private string CombineLogMessage(CountersEntity counters) =>
        string.Format(
            "Read: {0}; Calculated: {1}; Written: {2}",
            counters.ReadCounter,
            counters.CalculateCounter,
            counters.WriteCounter);

    public Task LogProgressAsync(CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            var currentCounters = Counters with { };
            while (!token.IsCancellationRequested)
            {
                if (currentCounters == Counters)
                {
                    Thread.Sleep(200);
                    continue;
                }
                currentCounters.ReadCounter = Counters.ReadCounter;
                currentCounters.WriteCounter = Counters.WriteCounter;
                currentCounters.CalculateCounter = Counters.CalculateCounter;

                _logger?.LogInformation(CombineLogMessage(currentCounters));
            }
        });
    }
}
