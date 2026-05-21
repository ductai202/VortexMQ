using DotnetBroker.Core.Protocol;
using DotnetBroker.Core.Queue;

namespace DotnetBroker.Core.Models;

/// <summary>
/// A named message channel. Owns its own ring-buffer queue and a list of consumer groups.
/// </summary>
public sealed class Topic(uint topicId)
{
    public uint TopicId { get; } = topicId;

    public RingBufferQueue<ProduceConsumePayload> Queue { get; } = new(capacity: 10_000);

    private readonly List<ConsumerGroup> _groups = [];
    private readonly SemaphoreSlim       _groupsLock = new(1, 1);

    public async Task<ConsumerGroup> GetOrAddGroupAsync(uint groupId, DeliveryMode mode)
    {
        await _groupsLock.WaitAsync();
        try
        {
            var existing = _groups.FirstOrDefault(g => g.GroupId == groupId);
            if (existing is not null) return existing;
            var newGroup = new ConsumerGroup(groupId, TopicId, mode);
            // New groups start at current queue position (don't replay history).
            newGroup.Offset = Queue.PopCount + Queue.Count;
            _groups.Add(newGroup);
            return newGroup;
        }
        finally { _groupsLock.Release(); }
    }

    public async Task<IReadOnlyList<ConsumerGroup>> GetGroupsSnapshotAsync()
    {
        await _groupsLock.WaitAsync();
        try   { return [.._groups]; }
        finally { _groupsLock.Release(); }
    }
}
