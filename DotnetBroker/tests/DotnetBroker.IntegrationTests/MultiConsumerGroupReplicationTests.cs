using DotnetBroker.Consumer;
using DotnetBroker.Core.Protocol;
using DotnetBroker.Producer;
using FluentAssertions;

namespace DotnetBroker.IntegrationTests;

/// <summary>
/// Verifies that 1 producer → 2 independent consumer groups each receive ALL messages.
/// This is the "fan-out" / replication behavior: each CG gets its own copy.
/// </summary>
public class MultiConsumerGroupReplicationTests : IAsyncDisposable
{
    private readonly BrokerFixture _broker = new();

    [Fact]
    public async Task OneProducer_TwoConsumerGroups_BothReceiveAllMessages()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var topic       = 1u;
        var group1      = 100u;
        var group2      = 200u;
        var prodPort    = BrokerFixture.GetFreePort();
        var cons1Port   = BrokerFixture.GetFreePort();
        var cons2Port   = BrokerFixture.GetFreePort();
        const int msgCount = 3;

        var received1 = new List<string>();
        var received2 = new List<string>();
        var done1 = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var done2 = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Consumer Group 1
        var consumer1 = new ConsumerClient(
            (ushort)cons1Port, topic, group1, DeliveryMode.Push,
            adminPort: _broker.AdminPort);
        await consumer1.ConnectAsync(cts.Token);
        _ = consumer1.ReceiveLoopAsync(
            onMessage: pcm =>
            {
                received1.Add(System.Text.Encoding.UTF8.GetString(pcm.Message));
                if (received1.Count >= msgCount) done1.TrySetResult(true);
            }, ct: cts.Token);

        // Consumer Group 2
        var consumer2 = new ConsumerClient(
            (ushort)cons2Port, topic, group2, DeliveryMode.Push,
            adminPort: _broker.AdminPort);
        await consumer2.ConnectAsync(cts.Token);
        _ = consumer2.ReceiveLoopAsync(
            onMessage: pcm =>
            {
                received2.Add(System.Text.Encoding.UTF8.GetString(pcm.Message));
                if (received2.Count >= msgCount) done2.TrySetResult(true);
            }, ct: cts.Token);

        await Task.Delay(300, cts.Token); // let advancers start

        // Producer sends messages
        var producer = new ProducerClient(
            (ushort)prodPort, topic, adminPort: _broker.AdminPort);
        await producer.ConnectAsync(cts.Token);

        for (var i = 0; i < msgCount; i++)
        {
            await producer.SendMessageAsync(
                System.Text.Encoding.UTF8.GetBytes($"MSG-{i}"), cts.Token);
        }

        // Both groups should receive all messages
        await Task.WhenAll(
            done1.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token),
            done2.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token));

        received1.Should().HaveCount(msgCount);
        received2.Should().HaveCount(msgCount);
        for (var i = 0; i < msgCount; i++)
        {
            received1.Should().Contain($"MSG-{i}");
            received2.Should().Contain($"MSG-{i}");
        }
    }

    public async ValueTask DisposeAsync() => await _broker.DisposeAsync();
}
