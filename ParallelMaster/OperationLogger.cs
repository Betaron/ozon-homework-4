using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using ParallelMaster.Enums;

namespace ParallelMaster;

internal class OperationLogger
{
    private Dictionary<OperationType, ulong> _counters;
    private readonly ILogger? _logger;
    private Channel<OperationType> _logChannel =
        Channel.CreateUnbounded<OperationType>(new UnboundedChannelOptions()
        {
            AllowSynchronousContinuations = true,
            SingleReader = true,
            SingleWriter = false
        });

    public ChannelWriter<OperationType> LogWriter { get => _logChannel.Writer; }

    public OperationLogger(ILogger? logger)
    {
        _logger = logger;
        _counters = new()
        {
            { OperationType.Read, 0 },
            { OperationType.Calculate, 0 },
            { OperationType.Write, 0 },
        };
    }

    private string CombineLogMessage() =>
        string.Format(
            "Read: {0}; Calculated: {1}; Written: {2}",
            _counters[OperationType.Read],
            _counters[OperationType.Calculate],
            _counters[OperationType.Write]);

    public Task LogProgressAsync(CancellationToken token)
    {
        return Task.Factory.StartNew(async () =>
        {
            var previousType = OperationType.Read;

            await foreach (var type in _logChannel.Reader.ReadAllAsync(token))
            {
                if (type != previousType)
                {
                    _logger?.LogInformation(CombineLogMessage());
                }
                _counters[type]++;
                previousType = type;
            }

            _logger?.LogInformation($"Done: {CombineLogMessage()}");
        }).Unwrap();
    }
}
