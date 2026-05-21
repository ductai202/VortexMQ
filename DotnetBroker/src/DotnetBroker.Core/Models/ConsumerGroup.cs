using System.Net.Sockets;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Core.Models;

/// <summary>
/// Tracks all consumers within one consumer group for a specific topic.
/// Maintains an absolute read offset and (for Pull mode) a ready-consumer channel.
/// </summary>
public sealed class ConsumerGroup(uint groupId, uint topicId, DeliveryMode mode)
{
    public uint         GroupId { get; } = groupId;
    public uint         TopicId { get; } = topicId;
    public DeliveryMode Mode    { get; } = mode;

    // Absolute offset — the next message index this group needs to consume.
    public long Offset;

    // Active consumer streams registered with this group.
    private readonly List<(ushort Port, NetworkStream Stream, bool Alive)> _consumers = [];
    private readonly SemaphoreSlim _consumersLock = new(1, 1);
    private int _nextConsumerIndex = 0;

    /// <summary>Number of consumers currently registered (including dead ones).</summary>
    public int ConsumerCount => _consumers.Count;

    // Pull-mode ready queue: holds 1 (signal) per ready consumer.
    public System.Threading.Channels.Channel<int> ReadyQueue { get; } =
        System.Threading.Channels.Channel.CreateUnbounded<int>();

    public async Task AddConsumerAsync(ushort port, NetworkStream stream)
    {
        await _consumersLock.WaitAsync();
        try   { _consumers.Add((port, stream, true)); }
        finally { _consumersLock.Release(); }
    }

    /// <summary>
    /// Returns the next alive consumer in round-robin order, or null if none available.
    /// </summary>
    public async Task<(int Index, NetworkStream Stream)?> TryGetConsumerAsync()
    {
        await _consumersLock.WaitAsync();
        try
        {
            if (_consumers.Count == 0) return null;
            // Round-robin: try each consumer starting from _nextConsumerIndex
            var start = _nextConsumerIndex % _consumers.Count;
            for (var i = 0; i < _consumers.Count; i++)
            {
                var idx = (start + i) % _consumers.Count;
                if (_consumers[idx].Alive)
                {
                    _nextConsumerIndex = (idx + 1) % _consumers.Count;
                    return (idx, _consumers[idx].Stream);
                }
            }
            return null;
        }
        finally { _consumersLock.Release(); }
    }

    public async Task MarkDeadAsync(int index)
    {
        await _consumersLock.WaitAsync();
        try
        {
            if (index < _consumers.Count)
            {
                var c = _consumers[index];
                _consumers[index] = (c.Port, c.Stream, false);
            }
        }
        finally { _consumersLock.Release(); }
    }

    /// <summary>Returns snapshot of all alive consumer streams.</summary>
    public async Task<List<NetworkStream>> GetAliveStreamsAsync()
    {
        await _consumersLock.WaitAsync();
        try
        {
            return _consumers
                .Where(c => c.Alive)
                .Select(c => c.Stream)
                .ToList();
        }
        finally { _consumersLock.Release(); }
    }
}
