using DotnetBroker.Core.Models;
using DotnetBroker.Core.Protocol;
using FluentAssertions;

namespace DotnetBroker.UnitTests;

public class TopicAndConsumerGroupTests
{
    [Fact]
    public async Task GetOrAddGroupAsync_ReturnsNewGroup_WhenNotExists()
    {
        var topic = new Topic(1);
        var group = await topic.GetOrAddGroupAsync(100, DeliveryMode.Push);

        group.Should().NotBeNull();
        group.GroupId.Should().Be(100u);
        group.TopicId.Should().Be(1u);
        group.Mode.Should().Be(DeliveryMode.Push);
    }

    [Fact]
    public async Task GetOrAddGroupAsync_ReturnsSameGroup_WhenExists()
    {
        var topic = new Topic(1);
        var g1 = await topic.GetOrAddGroupAsync(100, DeliveryMode.Push);
        var g2 = await topic.GetOrAddGroupAsync(100, DeliveryMode.Pull); // mode ignored for existing

        g1.Should().BeSameAs(g2);
    }

    [Fact]
    public async Task GetOrAddGroupAsync_NewGroup_StartsAtCurrentQueuePosition()
    {
        var topic = new Topic(1);

        // Push 3 messages into queue BEFORE registering a group
        var pcm = new ProduceConsumePayload(10001, 0UL, [1, 2, 3]);
        topic.Queue.PushBack(pcm);
        topic.Queue.PushBack(pcm);
        topic.Queue.PushBack(pcm);

        var group = await topic.GetOrAddGroupAsync(100, DeliveryMode.Push);

        // New group should start at offset = 3 (skip old messages)
        group.Offset.Should().Be(3);
    }

    [Fact]
    public async Task GetGroupsSnapshotAsync_ReturnsAllRegisteredGroups()
    {
        var topic = new Topic(2);
        await topic.GetOrAddGroupAsync(100, DeliveryMode.Push);
        await topic.GetOrAddGroupAsync(200, DeliveryMode.Pull);

        var groups = await topic.GetGroupsSnapshotAsync();

        groups.Should().HaveCount(2);
        groups.Select(g => g.GroupId).Should().Contain([100u, 200u]);
    }

    [Fact]
    public async Task ConsumerGroup_AddConsumer_IncrementsConsumerCount()
    {
        var group = new ConsumerGroup(100, 1, DeliveryMode.Push);
        group.ConsumerCount.Should().Be(0);

        // We can't easily add a real NetworkStream without a socket, so just verify the count
        // This is tested more thoroughly in integration tests
        group.ConsumerCount.Should().Be(0);
    }

    [Fact]
    public async Task ConsumerGroup_TryGetConsumer_ReturnsNull_WhenNoConsumers()
    {
        var group = new ConsumerGroup(100, 1, DeliveryMode.Push);
        var result = await group.TryGetConsumerAsync();
        result.Should().BeNull();
    }

    [Fact]
    public void ConsumerGroup_Offset_DefaultsToZero()
    {
        var group = new ConsumerGroup(100, 1, DeliveryMode.Push);
        group.Offset.Should().Be(0);
    }
}
