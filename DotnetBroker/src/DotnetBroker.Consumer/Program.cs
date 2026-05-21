// DotnetBroker.Consumer — entry point
// Usage: dotnet run -- <port> <topic> <group_id> [push|pull] [adminHost] [adminPort]
//   port      : local TCP server port this consumer will listen on
//   topic     : topic ID to consume from (uint)
//   group_id  : consumer group ID (uint)
//   mode      : push (default) or pull
//   adminHost : KAdmin host (default 127.0.0.1)
//   adminPort : KAdmin port (default 10000)

using DotnetBroker.Consumer;
using DotnetBroker.Core.Protocol;

var localPort = ushort.Parse(args.ElementAtOrDefault(0) ?? "10002");
var topicId   = uint.Parse(args.ElementAtOrDefault(1)   ?? "1");
var groupId   = uint.Parse(args.ElementAtOrDefault(2)   ?? "100");
var modeStr   = args.ElementAtOrDefault(3)?.ToLower();
var mode      = modeStr == "pull" ? DeliveryMode.Pull : DeliveryMode.Push;
var adminHost = args.ElementAtOrDefault(4) ?? "127.0.0.1";
var adminPort = int.Parse(args.ElementAtOrDefault(5)    ?? "10000");

Console.WriteLine($"[Consumer] port={localPort} topic={topicId} group={groupId} mode={mode} admin={adminHost}:{adminPort}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var client = new ConsumerClient(localPort, topicId, groupId, mode, adminHost, adminPort);
await client.ConnectAsync(cts.Token);
await client.ReceiveLoopAsync(ct: cts.Token);
