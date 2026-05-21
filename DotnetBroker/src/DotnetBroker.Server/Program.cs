// DotnetBroker.Server — KAdmin entry point
// Usage: dotnet run [-- [--port <port>] [--data <dataDir>]]
//   --port  : Admin TCP port (default 10000)
//   --data  : Persistence directory (default "broker_data")

using DotnetBroker.Server;

var port    = 10000;
var dataDir = "broker_data";

for (var i = 0; i < args.Length; i++)
{
    if (args[i] == "--port" && i + 1 < args.Length) port    = int.Parse(args[++i]);
    if (args[i] == "--data" && i + 1 < args.Length) dataDir = args[++i];
}

Console.WriteLine($"[DotnetBroker.Server] KAdmin starting on port {port}, data dir: {dataDir}");
Console.CancelKeyPress += (_, e) => { e.Cancel = true; Console.WriteLine("[KAdmin] Ctrl+C received — shutting down..."); };

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var server = new BrokerServer(port, dataDir);
await server.RunAsync(cts.Token);
