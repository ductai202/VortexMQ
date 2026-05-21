using BenchmarkDotNet.Attributes;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Benchmarks;

/// <summary>
/// Measures serialization/deserialization throughput for the wire protocol payloads.
/// All operations are allocation-measured via MemoryDiagnoser.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 3, warmupCount: 1)]
public class LatencyBenchmark
{
    private ProducerRegisterPayload _pReg;
    private ConsumerRegisterPayload _cReg;
    private ProduceConsumePayload   _pcm;
    private byte[] _pRegBuf = null!;
    private byte[] _cRegBuf = null!;

    [GlobalSetup]
    public void Setup()
    {
        _pReg    = new ProducerRegisterPayload(Topic: 42, Port: 10001);
        _cReg    = new ConsumerRegisterPayload(Topic: 1, Port: 10002, GroupId: 100, Mode: DeliveryMode.Push);
        _pcm     = new ProduceConsumePayload(ProducerPort: 10001, Timestamp: 1234567890UL, Message: new byte[64]);
        _pRegBuf = new byte[ProducerRegisterPayload.Size];
        _cRegBuf = new byte[ConsumerRegisterPayload.Size];
    }

    [Benchmark]
    public void ProducerRegisterPayload_Encode()
    {
        _pReg.Encode(_pRegBuf);
    }

    [Benchmark]
    public ProducerRegisterPayload ProducerRegisterPayload_Decode()
    {
        _pReg.Encode(_pRegBuf);
        return ProducerRegisterPayload.Decode(_pRegBuf);
    }

    [Benchmark]
    public void ConsumerRegisterPayload_Encode()
    {
        _cReg.Encode(_cRegBuf);
    }

    [Benchmark]
    public byte[] ProduceConsumePayload_Encode()
    {
        return _pcm.Encode();
    }

    [Benchmark]
    public ProduceConsumePayload ProduceConsumePayload_RoundTrip()
    {
        var encoded = _pcm.Encode();
        return ProduceConsumePayload.Decode(encoded);
    }

    /// <summary>Simulates the timestamp-delta latency calculation used in performance reporting.</summary>
    [Benchmark]
    public long MeasureTimestampDelta()
    {
        var sentTs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var receivedTs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return (long)(receivedTs - sentTs);
    }
}
