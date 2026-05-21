using DotnetBroker.Core.Queue;
using FluentAssertions;

namespace DotnetBroker.UnitTests;

public class RingBufferQueueTests
{
    [Fact]
    public void PushBack_And_PopFront_MaintainsFifoOrder()
    {
        var q = new RingBufferQueue<int>(capacity: 5);
        q.PushBack(10);
        q.PushBack(20);
        q.PushBack(30);

        q.PopFront().Should().Be(10);
        q.PopFront().Should().Be(20);
        q.PopFront().Should().Be(30);
        q.Count.Should().Be(0);
    }

    [Fact]
    public void Peek_WithAbsoluteOffset_ReturnsCorrectItem()
    {
        var q = new RingBufferQueue<string>(capacity: 4);
        q.PushBack("A");
        q.PushBack("B");
        q.PushBack("C");

        // Absolute offsets start at 0
        q.Peek(0).Should().Be("A");
        q.Peek(1).Should().Be("B");
        q.Peek(2).Should().Be("C");
        q.Peek(3).Should().BeNull(); // not pushed yet
    }

    [Fact]
    public void PushBack_WhenFull_ThrowsInvalidOperationException()
    {
        var q = new RingBufferQueue<int>(capacity: 3);
        q.PushBack(1);
        q.PushBack(2);
        q.PushBack(3);

        var act = () => q.PushBack(4);
        act.Should().Throw<InvalidOperationException>().WithMessage("*full*");
    }

    [Fact]
    public void PopFront_IncrementsPopCount()
    {
        var q = new RingBufferQueue<int>(capacity: 5);
        q.PushBack(1);
        q.PushBack(2);
        q.PushBack(3);

        q.PopCount.Should().Be(0);
        q.PopFront();
        q.PopCount.Should().Be(1);
        q.PopFront();
        q.PopCount.Should().Be(2);
    }

    [Fact]
    public void Peek_AfterPop_UsesAbsoluteOffset()
    {
        var q = new RingBufferQueue<int>(capacity: 5);
        q.PushBack(100);
        q.PushBack(200);
        q.PushBack(300);

        q.PopFront(); // pops 100, PopCount=1

        // Absolute offset 0 was popped — should return default
        q.Peek(0).Should().Be(default);
        // Absolute offset 1 is now 200
        q.Peek(1).Should().Be(200);
        // Absolute offset 2 is 300
        q.Peek(2).Should().Be(300);
    }

    [Fact]
    public void RingBuffer_Wraparound_WorksCorrectly()
    {
        var q = new RingBufferQueue<int>(capacity: 4);
        q.PushBack(1);
        q.PushBack(2);
        q.PushBack(3);
        q.PushBack(4);

        q.PopFront(); // pop 1 → count=3, PopCount=1
        q.PopFront(); // pop 2 → count=2, PopCount=2

        // Now push 2 more — should wrap around in array
        q.PushBack(5);
        q.PushBack(6);

        q.PopFront().Should().Be(3);
        q.PopFront().Should().Be(4);
        q.PopFront().Should().Be(5);
        q.PopFront().Should().Be(6);
    }

    [Fact]
    public void PopFront_OnEmptyQueue_ReturnsDefault()
    {
        var q = new RingBufferQueue<int>(capacity: 3);
        q.PopFront().Should().Be(default);
    }

    [Fact]
    public async Task ConcurrentPushAndPeek_IsThreadSafe()
    {
        var q = new RingBufferQueue<int>(capacity: 1000);
        var pushTask = Task.Run(() =>
        {
            for (var i = 0; i < 500; i++) q.PushBack(i);
        });
        var peekTask = Task.Run(() =>
        {
            for (var i = 0; i < 200; i++) _ = q.Peek(i);
        });

        var act = async () => await Task.WhenAll(pushTask, peekTask)
            .WaitAsync(TimeSpan.FromSeconds(5));
        await act.Should().NotThrowAsync();
    }
}
