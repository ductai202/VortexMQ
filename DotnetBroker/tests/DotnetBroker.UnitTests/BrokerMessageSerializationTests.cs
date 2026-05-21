using DotnetBroker.Core.Protocol;
using FluentAssertions;

namespace DotnetBroker.UnitTests;

public class BrokerMessageSerializationTests
{
    [Fact]
    public void ProducerRegisterPayload_RoundTrips_Correctly()
    {
        // Arrange
        var payload = new ProducerRegisterPayload(Topic: 42, Port: 12345);
        var buf = new byte[ProducerRegisterPayload.Size];

        // Act
        payload.Encode(buf);
        var decoded = ProducerRegisterPayload.Decode(buf);

        // Assert
        decoded.Should().Be(payload);
        decoded.Topic.Should().Be(42u);
        decoded.Port.Should().Be(12345);
    }

    [Fact]
    public void ConsumerRegisterPayload_PushMode_RoundTrips_Correctly()
    {
        var payload = new ConsumerRegisterPayload(Topic: 7, Port: 9999, GroupId: 100, Mode: DeliveryMode.Push);
        var buf = new byte[ConsumerRegisterPayload.Size];

        payload.Encode(buf);
        var decoded = ConsumerRegisterPayload.Decode(buf);

        decoded.Should().Be(payload);
        decoded.Mode.Should().Be(DeliveryMode.Push);
    }

    [Fact]
    public void ConsumerRegisterPayload_PullMode_RoundTrips_Correctly()
    {
        var payload = new ConsumerRegisterPayload(Topic: 3, Port: 8888, GroupId: 200, Mode: DeliveryMode.Pull);
        var buf = new byte[ConsumerRegisterPayload.Size];

        payload.Encode(buf);
        var decoded = ConsumerRegisterPayload.Decode(buf);

        decoded.Should().Be(payload);
        decoded.Mode.Should().Be(DeliveryMode.Pull);
    }

    [Fact]
    public void ProduceConsumePayload_RoundTrips_Correctly()
    {
        var messageBody = System.Text.Encoding.UTF8.GetBytes("Hello, Broker!");
        var payload = new ProduceConsumePayload(
            ProducerPort: 10001,
            Timestamp:    1234567890UL,
            Message:      messageBody);

        var encoded = payload.Encode();
        var decoded = ProduceConsumePayload.Decode(encoded);

        decoded.ProducerPort.Should().Be(10001);
        decoded.Timestamp.Should().Be(1234567890UL);
        decoded.Message.Should().Equal(messageBody);
    }

    [Fact]
    public void ProduceConsumePayload_EmptyMessage_RoundTrips()
    {
        var payload = new ProduceConsumePayload(ProducerPort: 0, Timestamp: 0, Message: []);
        var encoded = payload.Encode();
        var decoded = ProduceConsumePayload.Decode(encoded);
        decoded.Message.Should().BeEmpty();
    }

    [Theory]
    [InlineData(0u, 0, 0u, DeliveryMode.Push)]
    [InlineData(uint.MaxValue, ushort.MaxValue, uint.MaxValue, DeliveryMode.Pull)]
    [InlineData(1u, 10000, 100u, DeliveryMode.Push)]
    public void ConsumerRegisterPayload_BoundaryValues_RoundTrip(uint topic, int port, uint groupId, DeliveryMode mode)
    {
        var payload = new ConsumerRegisterPayload(topic, (ushort)port, groupId, mode);
        var buf = new byte[ConsumerRegisterPayload.Size];
        payload.Encode(buf);
        var decoded = ConsumerRegisterPayload.Decode(buf);
        decoded.Should().Be(payload);
    }
}
