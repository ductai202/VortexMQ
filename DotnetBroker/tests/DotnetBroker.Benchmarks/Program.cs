// DotnetBroker.Benchmarks — run with: dotnet run -c Release
// BenchmarkDotNet will print statistical results (mean, stddev, P99, alloc)

using BenchmarkDotNet.Running;
using DotnetBroker.Benchmarks;

// Quick mode: if not in Release, run quick summary
BenchmarkSwitcher.FromAssembly(typeof(RingBufferBenchmark).Assembly).RunAll();
