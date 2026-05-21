// DotnetBroker.Producer — entry point
// Usage: dotnet run -- <port> <topic> [adminHost] [adminPort]
//   port      : local TCP server port this producer will listen on (e.g. 10001)
//   topic     : topic ID to publish to (uint)
//   adminHost : KAdmin host (default 127.0.0.1)
//   adminPort : KAdmin port (default 10000)

using DotnetBroker.Core.Protocol;
using DotnetBroker.Producer;

var localPort = ushort.Parse(args.ElementAtOrDefault(0) ?? "10001");
var topicId   = uint.Parse(args.ElementAtOrDefault(1)   ?? "1");
var adminHost = args.ElementAtOrDefault(2) ?? "127.0.0.1";
var adminPort = int.Parse(args.ElementAtOrDefault(3)    ?? "10000");

Console.WriteLine($"[Producer] port={localPort} topic={topicId} admin={adminHost}:{adminPort}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var client = new ProducerClient(localPort, topicId, adminHost, adminPort);
await client.ConnectAsync(cts.Token);
await client.RunInteractiveAsync(cts.Token);
