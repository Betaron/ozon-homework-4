namespace ParallelMaster;

public record CountersEntity
{
    public ulong ReadCounter { get; set; }
    public ulong CalculateCounter { get; set; }
    public ulong WriteCounter { get; set; }
}
