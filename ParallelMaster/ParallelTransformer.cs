using System.Collections.Concurrent;
using System.Threading.Channels;
using CsvHelper;
using Microsoft.Extensions.Logging;
using ParallelMaster.Enums;
using ParallelMaster.Extentions;

namespace ParallelMaster;
public class ParallelTransformer<TInput, TOutput> : IDisposable
    where TInput : class
    where TOutput : class
{
    private uint _parallelismDegree;

    private Stack<CancellationTokenSource> _threadCts = new();
    private CancellationTokenSource _readInputCts = new();
    private CancellationTokenSource _writeOutputCts = new();
    private CancellationTokenSource _countCts = new();
    private List<Task> _calculationTasks = new();
    private Task _readInputTask;
    private Task _writeOutputTask;
    private Task _countTask;
    private BlockingCollection<TInput> _inputBuffer;
    private BlockingCollection<TOutput> _outputBuffer;
    private CsvFileWorker _fileWorker;
    private Channel<OperationsTypes> _logChanel;

    private Dictionary<OperationsTypes, ulong> _counters;

    public Func<TInput, TOutput> TransformFunction { get; init; }
    public string InputPath { get; init; }
    public string OutputPath { get; init; }
    public int InputBufferSize { get; init; }
    public bool IsExecuting { get; private set; }
    public ILogger? Logger { get; set; }
    public uint ParallelismDegree
    {
        get => _parallelismDegree;
        set
        {
            if (value > 0)
            {
                _parallelismDegree = value;
            }

            if (IsExecuting)
            {
                OnParallelismDegreeChanged();
            }
        }
    }

    public ParallelTransformer(
        Func<TInput, TOutput> transformFunction,
        string inputPath,
        string? outputPath = null,
        uint parallelismDegree = 1,
        int inputBufferSize = 1,
        ILogger? logger = null)
    {
        TransformFunction = transformFunction;
        InputPath = inputPath;
        OutputPath = outputPath ?? inputPath.Insert(inputPath.LastIndexOf('.'), "_calculated");
        InputBufferSize = inputBufferSize < 1 ? 1 : inputBufferSize;
        ParallelismDegree = parallelismDegree;
        Logger = logger;

        _inputBuffer = new BlockingCollection<TInput>(inputBufferSize);
        _outputBuffer = new BlockingCollection<TOutput>();
        _fileWorker = new(InputPath, OutputPath);
        _logChanel = Channel.CreateUnbounded<OperationsTypes>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            AllowSynchronousContinuations = true,
        });
        _counters = new()
        {
            { OperationsTypes.Read, 0 },
            { OperationsTypes.Calculate, 0 },
            { OperationsTypes.Write, 0 },
        };
    }

    private void OnParallelismDegreeChanged()
    {
        var difference = ParallelismDegree - _calculationTasks.Count;

        if (difference > 0)
        {
            for (var i = 0; i < difference; i++)
            {
                StartNewThread();
            }
        }

        if (difference < 0)
        {
            for (var i = difference; i >= 0; i++)
            {
                _threadCts.Pop().Cancel();
            }
        }
    }

    public async Task Execute()
    {
        if (IsExecuting)
        {
            throw new InvalidOperationException(message:
                "Execution already started.");
        }

        IsExecuting = true;

        _counters[OperationsTypes.Read] = 0;
        _counters[OperationsTypes.Calculate] = 0;
        _counters[OperationsTypes.Write] = 0;

        _readInputTask = ReadInBufferAsync(_fileWorker.CsvReader, _readInputCts.Token);
        _writeOutputTask = WriteFromBufferAsync(_fileWorker.CsvWriter, _writeOutputCts.Token);

        for (int i = 0; i < ParallelismDegree; i++)
        {
            StartNewThread();
        }

        _countTask = CountAndLogAsync(_countCts.Token);

        await Task.WhenAll(_calculationTasks);
        _outputBuffer.CompleteAdding();
        await _writeOutputTask;

        _calculationTasks.Clear();
        _threadCts.Clear();

        IsExecuting = false;
    }

    private void StartNewThread()
    {
        var tokenSource = new CancellationTokenSource();
        _threadCts.Push(tokenSource);
        _calculationTasks.Add(TransformAsync(tokenSource.Token));
    }

    private Task TransformAsync(
        CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            var channelWriter = (ChannelWriter<OperationsTypes>)_logChanel;

            while (!_inputBuffer.IsCompleted && !token.IsCancellationRequested)
            {
                TInput inputRecord;
                try { inputRecord = _inputBuffer.Take(); }
                catch (InvalidOperationException) { break; }

                var result = TransformFunction.Invoke(inputRecord);

                _outputBuffer.Add(result);

                channelWriter.WriteAsync(OperationsTypes.Calculate);
            }
        }, token);
    }

    private Task ReadInBufferAsync(CsvReader csvReader, CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            csvReader.Read();
            csvReader.ReadHeader();

            var channelWriter = (ChannelWriter<OperationsTypes>)_logChanel;

            while (_fileWorker.CsvReader.Read() && !token.IsCancellationRequested)
            {
                var record = csvReader.GetRecord<TInput>();
                if (record == null)
                {
                    continue;
                }

                _inputBuffer.Add(record);

                channelWriter.WriteAsync(OperationsTypes.Read);
            }
            _inputBuffer.CompleteAdding();
        }, token);
    }

    private Task WriteFromBufferAsync(CsvWriter csvWriter, CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            csvWriter.UseSnakeCaseHeaders<TOutput>();

            csvWriter.WriteHeader<TOutput>();
            csvWriter.NextRecord();
            csvWriter.Flush();

            var channelWriter = (ChannelWriter<OperationsTypes>)_logChanel;

            while (!_outputBuffer.IsCompleted && !token.IsCancellationRequested)
            {
                TOutput outputRecord;
                try { outputRecord = _outputBuffer.Take(); }
                catch (InvalidOperationException) { break; }

                csvWriter.WriteRecord(outputRecord);
                csvWriter.NextRecord();
                csvWriter.Flush();

                channelWriter.WriteAsync(OperationsTypes.Write);
            }
            channelWriter.Complete();
        }, token);
    }

    private async Task CountAndLogAsync(CancellationToken token)
    {
        var startTime = DateTime.UtcNow.TimeOfDay;
        var previousType = OperationsTypes.Read;
        var channelReader = (ChannelReader<OperationsTypes>)_logChanel;
        while (await channelReader.WaitToReadAsync() && !token.IsCancellationRequested)
        {
            while (channelReader.TryRead(out OperationsTypes type) && !token.IsCancellationRequested)
            {
                if (type != previousType)
                {
                    Logger?.LogInformation(string.Format(
                    "Read: {0}; Calculated: {1}; Written: {2}",
                    _counters[OperationsTypes.Read],
                    _counters[OperationsTypes.Calculate],
                    _counters[OperationsTypes.Write]));
                }

                _counters[type]++;
            }
        }
        Logger?.LogInformation($"Total time: {DateTime.UtcNow.TimeOfDay - startTime}");
    }

    public async void Dispose()
    {
        _readInputCts.Cancel();
        foreach (var item in _threadCts)
        {
            item.Cancel();
        }
        _writeOutputCts.Cancel();
        _countCts.Cancel();
        await _readInputTask;
        await Task.WhenAll(_calculationTasks);
        await _writeOutputTask;
        await _countTask;
        _inputBuffer.Dispose();
        _fileWorker.Dispose();

        GC.SuppressFinalize(this);
    }
}
