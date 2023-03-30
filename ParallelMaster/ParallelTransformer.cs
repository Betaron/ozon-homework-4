using CsvHelper;
using ParallelMaster.Extentions;

namespace ParallelMaster;
public class ParallelTransformer<TInput, TOutput> : IDisposable
    where TInput : class
    where TOutput : class
{
    private readonly Func<TInput, TOutput> _transformFunction;
    private readonly string _inputPath;
    private readonly string _outputPath;
    private uint _parallelismDegree;

    private readonly object _inputLock = new();
    private readonly object _outputLock = new();

    private Stack<CancellationTokenSource> _cts = new();
    private List<Task> _tasks = new();
    private FileWorker _fileWorker;

    public bool IsExecuting { get; set; }
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
        uint parallelismDegree = 1)
    {
        _transformFunction = transformFunction;
        _inputPath = inputPath;
        _outputPath = outputPath ?? inputPath.Insert(inputPath.LastIndexOf('.'), "_calculated");
        ParallelismDegree = parallelismDegree;

        _fileWorker = new(_inputPath, _outputPath);
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
                _cts.Pop().Cancel();
            }
        }
    }

    public async Task Execute()
    {
        IsExecuting = true;

        _fileWorker.CsvReader.Read();
        _fileWorker.CsvReader.ReadHeader();

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
        _cts.Clear();
        IsExecuting = false;
    }

    private void StartNewThread()
    {
        var tokenSource = new CancellationTokenSource();
        _cts.Push(tokenSource);
        _tasks.Add(GetTransformTask(
            _fileWorker.CsvReader,
            _fileWorker.CsvWriter,
            tokenSource.Token));
    }

    private Task GetTransformTask(
        CsvReader csvReader,
        CsvWriter csvWriter,
        CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            while (!token.IsCancellationRequested)
            {
                TInput? inputRecord = null;
                lock (_inputLock)
                {
                    if (csvReader.Read())
                    {
                        inputRecord = csvReader.GetRecord<TInput>();
                    }
                }
                if (inputRecord == null) { break; }

                var result = _transformFunction.Invoke(inputRecord);

                lock (_outputLock)
                {
                    csvWriter.WriteRecord(result);
                    csvWriter.NextRecord();
                    csvWriter.Flush();
                }
            }
        }, token);
    }

    public void Dispose()
    {
        foreach (var item in _cts)
        {
            item.Cancel();
        }
        Task.WaitAll(_tasks.ToArray());
        _fileWorker.Dispose();
    }
}
