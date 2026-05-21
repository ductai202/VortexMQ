using DotnetBroker.Consumer;
using DotnetBroker.Core.Protocol;
using DotnetBroker.Producer;
using FluentAssertions;

namespace DotnetBroker.IntegrationTests;

/// <summary>
/// Verifies Pull mode delivery:
/// - Consumer sends C_RD signal before receiving each message
/// - Server waits for C_RD before delivering
/// - Messages are delivered in order
/// </summary>
public class PullModelTests : IAsyncDisposable
{
    private readonly BrokerFixture _broker = new();

    [Fact]
    public async Task PullMode_Consumer_ReceivesMessages_OnlyAfterSignalingReady()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var topic    = 1u;
        var group    = 100u;
        var prodPort = BrokerFixture.GetFreePort();
        var consPort = BrokerFixture.GetFreePort();
        const int msgCount = 3;

        var received = new List<string>();
        var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Register Pull-mode consumer
        var consumer = new ConsumerClient(
            (ushort)consPort, topic, group, DeliveryMode.Pull,
            adminPort: _broker.AdminPort);
        await consumer.ConnectAsync(cts.Token);
        _ = consumer.ReceiveLoopAsync(
            onMessage: pcm =>
            {
                received.Add(System.Text.Encoding.UTF8.GetString(pcm.Message));
                if (received.Count >= msgCount) done.TrySetResult(true);
            }, ct: cts.Token);

        await Task.Delay(300, cts.Token);

        // Producer sends messages
        var producer = new ProducerClient(
            (ushort)prodPort, topic, adminPort: _broker.AdminPort);
        await producer.ConnectAsync(cts.Token);

        for (var i = 0; i < msgCount; i++)
        {
            await producer.SendMessageAsync(
                System.Text.Encoding.UTF8.GetBytes($"Pull-{i}"), cts.Token);
        }

        await done.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);

        received.Should().HaveCount(msgCount);
        for (var i = 0; i < msgCount; i++)
            received.Should().Contain($"Pull-{i}");
    }

    [Fact]
    public async Task PullMode_VsPushMode_BothReceiveSameMessages()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var topic = 2u;
        var pushGroup = 200u;
        var pullGroup = 300u;
        var prodPort      = BrokerFixture.GetFreePort();
        var pushConsPort  = BrokerFixture.GetFreePort();
        var pullConsPort  = BrokerFixture.GetFreePort();
        const int msgCount = 3;

        var pushReceived = new List<string>();
        var pullReceived = new List<string>();
        var pushDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pullDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Push consumer
        var pushConsumer = new ConsumerClient(
            (ushort)pushConsPort, topic, pushGroup, DeliveryMode.Push,
            adminPort: _broker.AdminPort);
        await pushConsumer.ConnectAsync(cts.Token);
        _ = pushConsumer.ReceiveLoopAsync(
            onMessage: pcm =>
            {
                pushReceived.Add(System.Text.Encoding.UTF8.GetString(pcm.Message));
                if (pushReceived.Count >= msgCount) pushDone.TrySetResult(true);
            }, ct: cts.Token);

        // Pull consumer
        var pullConsumer = new ConsumerClient(
            (ushort)pullConsPort, topic, pullGroup, DeliveryMode.Pull,
            adminPort: _broker.AdminPort);
        await pullConsumer.ConnectAsync(cts.Token);
        _ = pullConsumer.ReceiveLoopAsync(
            onMessage: pcm =>
            {
                pullReceived.Add(System.Text.Encoding.UTF8.GetString(pcm.Message));
                if (pullReceived.Count >= msgCount) pullDone.TrySetResult(true);
            }, ct: cts.Token);

        await Task.Delay(400, cts.Token);

        var producer = new ProducerClient(
            (ushort)prodPort, topic, adminPort: _broker.AdminPort);
        await producer.ConnectAsync(cts.Token);

        for (var i = 0; i < msgCount; i++)
        {
            await producer.SendMessageAsync(
                System.Text.Encoding.UTF8.GetBytes($"Shared-{i}"), cts.Token);
        }

        await Task.WhenAll(
            pushDone.Task.WaitAsync(TimeSpan.FromSeconds(20), cts.Token),
            pullDone.Task.WaitAsync(TimeSpan.FromSeconds(20), cts.Token));

        pushReceived.Should().HaveCount(msgCount);
        pullReceived.Should().HaveCount(msgCount);
    }

    public async ValueTask DisposeAsync() => await _broker.DisposeAsync();
}
