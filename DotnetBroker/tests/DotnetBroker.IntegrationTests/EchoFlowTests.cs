using System.Net;
using System.Net.Sockets;
using DotnetBroker.Core.Protocol;
using FluentAssertions;

namespace DotnetBroker.IntegrationTests;

/// <summary>
/// Basic TCP echo test — verifies that the server handles ECHO message type correctly.
/// </summary>
public class EchoFlowTests : IAsyncDisposable
{
    private readonly BrokerFixture _broker = new();

    [Fact]
    public async Task Echo_SendAndReceive_RoundTrip()
    {
        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", _broker.AdminPort);
        client.NoDelay = true;
        var stream = client.GetStream();

        var message = "Hello DotnetBroker!";
        await stream.WriteStringAsync(MessageType.Echo, message);

        var (type, payload) = await stream.ReadMessageAsync();
        type.Should().Be(MessageType.R_Echo);
        System.Text.Encoding.UTF8.GetString(payload).Should().Be(message);
    }

    [Fact]
    public async Task Echo_MultipleMessages_AllRoundTrip()
    {
        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", _broker.AdminPort);
        client.NoDelay = true;
        var stream = client.GetStream();

        var messages = new[] { "Hello", "World", "Broker" };
        foreach (var msg in messages)
        {
            await stream.WriteStringAsync(MessageType.Echo, msg);
            var (type, payload) = await stream.ReadMessageAsync();
            type.Should().Be(MessageType.R_Echo);
            System.Text.Encoding.UTF8.GetString(payload).Should().Be(msg);
        }
    }

    public async ValueTask DisposeAsync() => await _broker.DisposeAsync();
}
