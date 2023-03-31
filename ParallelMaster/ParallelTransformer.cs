using System.Collections.Concurrent;
using CsvHelper;
using ParallelMaster.Extentions;

namespace ParallelMaster;
public class ParallelTransformer<TInput, TOutput> : IDisposable
    where TInput : class
    where TOutput : class
{
    private uint _parallelismDegree;

    private readonly object _inputLock = new();
    private readonly object _outputLock = new();

    private Stack<CancellationTokenSource> _threadCts = new();
    private CancellationTokenSource _readInputCts = new();
    private CancellationTokenSource _takeInputCts = new();
    private List<Task> _tasks = new();
    private BlockingCollection<TInput> _inputBuffer;
    private CsvFileWorker _fileWorker;

    public Func<TInput, TOutput> TransformFunction { get; init; }
    public string InputPath { get; init; }
    public string OutputPath { get; init; }
    public int InputBufferSize { get; init; }
    public bool IsExecuting { get; private set; }
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
        int inputBufferSize = 1)
    {
        TransformFunction = transformFunction;
        InputPath = inputPath;
        OutputPath = outputPath ?? inputPath.Insert(inputPath.LastIndexOf('.'), "_calculated");
        ParallelismDegree = parallelismDegree;
        InputBufferSize = inputBufferSize;

        _inputBuffer = new BlockingCollection<TInput>(inputBufferSize);
        _fileWorker = new(InputPath, OutputPath);
    }

    private void OnParallelismDegreeChanged()
    {
        var difference = ParallelismDegree - _tasks.Count;

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

        ReadInLimitBufferAsync(_fileWorker.CsvReader, _readInputCts.Token);

        _fileWorker.CsvWriter.UseSnakeCaseHeaders<TOutput>();

        _fileWorker.CsvWriter.WriteHeader<TOutput>();
        _fileWorker.CsvWriter.NextRecord();
        _fileWorker.CsvWriter.Flush();

        for (int i = 0; i < ParallelismDegree; i++)
        {
            StartNewThread();
        }

        await Task.WhenAll(_tasks);
        _tasks.Clear();
        _threadCts.Clear();
        IsExecuting = false;
    }

    private void StartNewThread()
    {
        var tokenSource = new CancellationTokenSource();
        _threadCts.Push(tokenSource);
        _tasks.Add(TransformAsync(
            _fileWorker.CsvReader,
            _fileWorker.CsvWriter,
            tokenSource.Token));
    }

    private Task TransformAsync(
        CsvReader csvReader,
        CsvWriter csvWriter,
        CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            while (!token.IsCancellationRequested)
            {
                TInput inputRecord;

                try
                {
                    inputRecord = _inputBuffer.Take(_takeInputCts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                var result = TransformFunction.Invoke(inputRecord);

                lock (_outputLock)
                {
                    csvWriter.WriteRecord(result);
                    csvWriter.NextRecord();
                    csvWriter.Flush();
                }
            }
        }, token);
    }

    private Task ReadInLimitBufferAsync(CsvReader csvReader, CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            csvReader.Read();
            csvReader.ReadHeader();

            while (_fileWorker.CsvReader.Read() && !token.IsCancellationRequested)
            {
                _inputBuffer.Add(csvReader.GetRecord<TInput>());
            }
            _takeInputCts.Cancel();
        }, token);
    }

    public void Dispose()
    {
        foreach (var item in _threadCts)
        {
            item.Cancel();
        }
        _readInputCts.Cancel();
        Task.WaitAll(_tasks.ToArray());
        _inputBuffer.Dispose();
        _fileWorker.Dispose();
    }
}
