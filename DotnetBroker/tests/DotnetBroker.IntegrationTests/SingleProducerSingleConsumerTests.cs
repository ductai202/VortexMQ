using DotnetBroker.Consumer;
using DotnetBroker.Core.Protocol;
using DotnetBroker.Producer;
using FluentAssertions;

namespace DotnetBroker.IntegrationTests;

/// <summary>
/// End-to-end test: 1 producer → broker → 1 consumer, Push mode.
/// Verifies that a message sent by the producer is received by the consumer.
/// </summary>
public class SingleProducerSingleConsumerTests : IAsyncDisposable
{
    private readonly BrokerFixture _broker = new();

    [Fact]
    public async Task Producer_SendPCM_Consumer_Receives_InPushMode()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var topic  = 1u;
        var group  = 100u;
        var prodPort   = BrokerFixture.GetFreePort();
        var consPort   = BrokerFixture.GetFreePort();

        var received = new TaskCompletionSource<ProduceConsumePayload>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // Start consumer first
        var consumer = new ConsumerClient(
            (ushort)consPort, topic, group, DeliveryMode.Push,
            adminPort: _broker.AdminPort);
        await consumer.ConnectAsync(cts.Token);

        var consumerTask = consumer.ReceiveLoopAsync(
            onMessage: pcm => received.TrySetResult(pcm),
            ct: cts.Token);

        // Give consumer advancer a moment to start
        await Task.Delay(200, cts.Token);

        // Start producer and send one message
        var producer = new ProducerClient(
            (ushort)prodPort, topic, adminPort: _broker.AdminPort);
        await producer.ConnectAsync(cts.Token);

        var testMessage = "Hello from producer!";
        await producer.SendMessageAsync(
            System.Text.Encoding.UTF8.GetBytes(testMessage), cts.Token);

        // Wait for consumer to receive it
        var pcmResult = await received.Task.WaitAsync(TimeSpan.FromSeconds(10), cts.Token);

        System.Text.Encoding.UTF8.GetString(pcmResult.Message).Should().Be(testMessage);
        pcmResult.ProducerPort.Should().Be((ushort)prodPort);
    }

    [Fact]
    public async Task Producer_SendMultiplePCM_Consumer_ReceivesAll()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var topic  = 2u;
        var group  = 200u;
        var prodPort   = BrokerFixture.GetFreePort();
        var consPort   = BrokerFixture.GetFreePort();
        const int messageCount = 5;

        var receivedMessages = new List<string>();
        var allReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumer = new ConsumerClient(
            (ushort)consPort, topic, group, DeliveryMode.Push,
            adminPort: _broker.AdminPort);
        await consumer.ConnectAsync(cts.Token);

        var consumerTask = consumer.ReceiveLoopAsync(
            onMessage: pcm =>
            {
                receivedMessages.Add(System.Text.Encoding.UTF8.GetString(pcm.Message));
                if (receivedMessages.Count >= messageCount)
                    allReceived.TrySetResult(true);
            },
            ct: cts.Token);

        await Task.Delay(200, cts.Token);

        var producer = new ProducerClient(
            (ushort)prodPort, topic, adminPort: _broker.AdminPort);
        await producer.ConnectAsync(cts.Token);

        for (var i = 0; i < messageCount; i++)
        {
            await producer.SendMessageAsync(
                System.Text.Encoding.UTF8.GetBytes($"Message-{i}"), cts.Token);
        }

        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);

        receivedMessages.Should().HaveCount(messageCount);
        for (var i = 0; i < messageCount; i++)
            receivedMessages.Should().Contain($"Message-{i}");
    }

    public async ValueTask DisposeAsync() => await _broker.DisposeAsync();
}
