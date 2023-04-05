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

    private Queue<(CancellationTokenSource cts, Task task)> _threadCtsTasks = new();
    private CancellationTokenSource _readInputCts = new();
    private CancellationTokenSource _writeOutputCts = new();
    private CancellationTokenSource _countCts = new();
    private Task _readInputTask;
    private Task _writeOutputTask;
    private Task _countTask;
    private BlockingCollection<TInput> _inputBuffer;
    private BlockingCollection<TOutput> _outputBuffer;
    private CsvFileWorker _fileWorker;
    private Channel<OperationType> _logChanel;

    private Dictionary<OperationType, ulong> _counters;

    /// <summary>
    /// Функция трансформации данных.
    /// </summary>
    public Func<TInput, TOutput> TransformFunction { get; init; }

    /// <summary>
    /// Путь к файлу-источнику входных данных.
    /// </summary>
    public string InputPath { get; init; }

    /// <summary>
    /// Путь к файлу, куда будут сохранены результаты вычислений.
    /// </summary>
    public string OutputPath { get; init; }

    /// <summary>
    /// Размер буфера для хранения прочитанных данны. Максимум входных данных хранящихся в памяти.
    /// </summary>
    public int InputBufferSize { get; init; }

    /// <summary>
    /// Флаг исполнения.
    /// </summary>
    public bool IsExecuting { get; private set; }

    /// <summary>
    /// Логгер записывающий данные процесса исполнения.
    /// </summary>
    public ILogger? Logger { get; set; }

    /// <summary>
    /// Степень параллелизма. Сколько потоков будет одновременно производить расчеты. <br/>
    /// Изменение количества потоков применяется, также во время исполнения.
    /// </summary>
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
        _logChanel = Channel.CreateUnbounded<OperationType>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            AllowSynchronousContinuations = true,
        });
        _counters = new()
        {
            { OperationType.Read, 0 },
            { OperationType.Calculate, 0 },
            { OperationType.Write, 0 },
        };
    }

    private void OnParallelismDegreeChanged()
    {
        var difference = ParallelismDegree - _threadCtsTasks.Count;

        if (difference > 0)
        {
            for (var i = 0; i < difference; i++)
            {
                StartNewThread();
            }
        }

        if (difference < 0)
        {
            for (var i = difference; i < 0; i++)
            {
                _threadCtsTasks.Dequeue().cts.Cancel();
            }
        }
    }

    /// <summary>
    /// Метод запускает применение трансформаций данных в файле.
    /// </summary>
    /// <exception cref="InvalidOperationException">В один момент времени может быть запущен только один процесс.</exception>
    public async Task Execute()
    {
        if (IsExecuting)
        {
            throw new InvalidOperationException(message:
                "Execution already started.");
        }

        IsExecuting = true;

        _counters[OperationType.Read] = 0;
        _counters[OperationType.Calculate] = 0;
        _counters[OperationType.Write] = 0;

        _readInputTask = ReadInBufferAsync(_fileWorker.CsvReader, _readInputCts.Token);
        _writeOutputTask = WriteFromBufferAsync(_fileWorker.CsvWriter, _writeOutputCts.Token);

        for (int i = 0; i < ParallelismDegree; i++)
        {
            StartNewThread();
        }

        _countTask = CountAndLogAsync(_countCts.Token);

        WaitTransformTasks();

        _outputBuffer.CompleteAdding();
        await _writeOutputTask;

        _threadCtsTasks.Clear();

        IsExecuting = false;
    }

    private void StartNewThread()
    {
        var tokenSource = new CancellationTokenSource();
        _threadCtsTasks.Enqueue((tokenSource, TransformAsync(tokenSource.Token)));
    }

    private Task TransformAsync(CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            var channelWriter = (ChannelWriter<OperationType>)_logChanel;

            while (!_inputBuffer.IsCompleted && !token.IsCancellationRequested)
            {
                TInput inputRecord;
                try { inputRecord = _inputBuffer.Take(); }
                catch (InvalidOperationException) { break; }

                var result = TransformFunction.Invoke(inputRecord);

                _outputBuffer.Add(result);

                channelWriter.WriteAsync(OperationType.Calculate);
            }
        }, token);
    }

    private void WaitTransformTasks()
    {
        while (_threadCtsTasks.Count > 0)
        {
            _threadCtsTasks.First().task.Wait();
            _threadCtsTasks.Dequeue();
        }
    }

    private Task ReadInBufferAsync(CsvReader csvReader, CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            csvReader.Read();
            csvReader.ReadHeader();

            var channelWriter = (ChannelWriter<OperationType>)_logChanel;

            while (_fileWorker.CsvReader.Read() && !token.IsCancellationRequested)
            {
                var record = csvReader.GetRecord<TInput>();
                if (record == null)
                {
                    continue;
                }

                _inputBuffer.Add(record);

                channelWriter.WriteAsync(OperationType.Read);
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

            var channelWriter = (ChannelWriter<OperationType>)_logChanel;

            while (!_outputBuffer.IsCompleted && !token.IsCancellationRequested)
            {
                TOutput outputRecord;
                try { outputRecord = _outputBuffer.Take(); }
                catch (InvalidOperationException) { break; }

                csvWriter.WriteRecord(outputRecord);
                csvWriter.NextRecord();
                csvWriter.Flush();

                channelWriter.WriteAsync(OperationType.Write);
            }
            channelWriter.Complete();
        }, token);
    }

    private async Task CountAndLogAsync(CancellationToken token)
    {
        var startTime = DateTime.UtcNow.TimeOfDay;
        var previousType = OperationType.Read;
        var channelReader = (ChannelReader<OperationType>)_logChanel;
        while (await channelReader.WaitToReadAsync())
        {
            while (channelReader.TryRead(out OperationType type))
            {
                if (type != previousType)
                {
                    Logger?.LogInformation(CombineLogMessage());
                }

                _counters[type]++;
            }
        }
        Logger?.LogInformation($"Total time: {DateTime.UtcNow.TimeOfDay - startTime}" +
            Environment.NewLine +
            $"Done: {CombineLogMessage()}");
    }

    private string CombineLogMessage() =>
        string.Format(
                    "Read: {0}; Calculated: {1}; Written: {2}",
                    _counters[OperationType.Read],
                    _counters[OperationType.Calculate],
                    _counters[OperationType.Write]);

    public async void Dispose()
    {
        _readInputCts.Cancel();
        foreach (var item in _threadCtsTasks)
        {
            item.cts.Cancel();
            item.task.Wait();
        }
        _writeOutputCts.Cancel();
        _countCts.Cancel();
        await _readInputTask;
        await _writeOutputTask;
        await _countTask;
        _inputBuffer.Dispose();
        _fileWorker.Dispose();

        GC.SuppressFinalize(this);
    }
}
