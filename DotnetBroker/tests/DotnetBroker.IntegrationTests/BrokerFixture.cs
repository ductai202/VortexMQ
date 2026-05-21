using DotnetBroker.Server;

namespace DotnetBroker.IntegrationTests;

/// <summary>
/// Helper that starts a BrokerServer on a random free port and tears it down after the test.
/// Uses isolated data directories to avoid cross-test state pollution.
/// </summary>
public sealed class BrokerFixture : IAsyncDisposable
{
    public int AdminPort { get; }
    public BrokerServer Server { get; }
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _serverTask;
    private readonly string _dataDir;

    public BrokerFixture()
    {
        AdminPort = GetFreePort();
        _dataDir = Path.Combine(Path.GetTempPath(), "dotnetbroker_test_" + Guid.NewGuid().ToString("N")[..8]);
        Server = new BrokerServer(AdminPort, _dataDir);
        _serverTask = Task.Run(() => Server.RunAsync(_cts.Token));
        // Give the server a moment to bind
        Thread.Sleep(100);
    }

    public static int GetFreePort()
    {
        using var s = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        s.Start();
        var port = ((System.Net.IPEndPoint)s.LocalEndpoint).Port;
        s.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _serverTask.WaitAsync(TimeSpan.FromSeconds(5)); }
        catch { /* ignore cancellation */ }
        _cts.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }
}
