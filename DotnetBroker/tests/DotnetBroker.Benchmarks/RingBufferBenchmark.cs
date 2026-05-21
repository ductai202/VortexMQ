using BenchmarkDotNet.Attributes;
using DotnetBroker.Core.Queue;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Benchmarks;

/// <summary>
/// Raw ring-buffer benchmark — measures queue ops/sec without network overhead.
/// This is the baseline to understand in-memory throughput limits.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 3, warmupCount: 1)]
public class RingBufferBenchmark
{
    private RingBufferQueue<int> _intQueue = null!;
    private RingBufferQueue<ProduceConsumePayload> _pcmQueue = null!;
    private ProduceConsumePayload _payload;

    [Params(1_000, 10_000)]
    public int N;

    [GlobalSetup]
    public void Setup()
    {
        _intQueue  = new RingBufferQueue<int>(capacity: 100_000);
        _pcmQueue  = new RingBufferQueue<ProduceConsumePayload>(capacity: 100_000);
        _payload   = new ProduceConsumePayload(
            ProducerPort: 10001,
            Timestamp:    1_234_567_890UL,
            Message:      new byte[64]);
    }

    [Benchmark]
    public void PushPop_Integers()
    {
        for (var i = 0; i < N; i++)
            _intQueue.PushBack(i);
        for (var i = 0; i < N; i++)
            _intQueue.PopFront();
    }

    [Benchmark]
    public void PushPop_ProduceConsumePayload()
    {
        for (var i = 0; i < N; i++)
            _pcmQueue.PushBack(_payload);
        for (var i = 0; i < N; i++)
            _pcmQueue.PopFront();
    }

    [Benchmark]
    public void Peek_AtOffset()
    {
        // Fill queue first
        for (var i = 0; i < N; i++)
            _intQueue.PushBack(i);
        // Peek each item
        for (var i = 0; i < N; i++)
            _ = _intQueue.Peek(i);
        // Cleanup
        for (var i = 0; i < N; i++)
            _intQueue.PopFront();
    }
}
