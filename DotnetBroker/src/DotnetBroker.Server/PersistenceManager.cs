using System.Collections.Concurrent;
using System.Text.Json;
using DotnetBroker.Core.Models;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Server;

/// <summary>
/// Handles broker state persistence (Phase 7):
/// - Snapshot: topics + consumer group IDs saved as JSON
/// - Offset journal: per-CG binary append-only file recording consumed offsets
/// - Recovery: on startup, rebuild topics/CGs from snapshot + journals
/// </summary>
public sealed class PersistenceManager(string dataDir, ConcurrentDictionary<uint, Topic> topics)
{
    private readonly string _snapshotPath = Path.Combine(dataDir, "snapshot.json");
    private readonly SemaphoreSlim _snapshotLock = new(1, 1);

    // ---- Snapshot ----

    public async Task SnapshotAsync()
    {
        await _snapshotLock.WaitAsync();
        try
        {
            Directory.CreateDirectory(dataDir);
            var snapshot = new BrokerSnapshot
            {
                Topics = topics.Values.Select(async t => new TopicSnapshot
                {
                    TopicId   = t.TopicId,
                    GroupIds  = (await t.GetGroupsSnapshotAsync()).Select(g => g.GroupId).ToList(),
                    GroupModes = (await t.GetGroupsSnapshotAsync()).ToDictionary(g => g.GroupId, g => (byte)g.Mode)
                }).Select(t => t.Result).ToList()
            };

            var json = JsonSerializer.Serialize(snapshot, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(_snapshotPath, json);
            Console.WriteLine($"[Persistence] Snapshot saved: {snapshot.Topics.Count} topic(s)");

            // Save offsets per CG
            foreach (var topic in topics.Values)
            {
                var groups = await topic.GetGroupsSnapshotAsync();
                foreach (var g in groups)
                {
                    await WriteOffsetJournalAsync(g.GroupId, g.Offset);
                }
            }
        }
        catch (Exception ex) { Console.WriteLine($"[Persistence] Snapshot failed: {ex.Message}"); }
        finally { _snapshotLock.Release(); }
    }

    public async Task RestoreAsync()
    {
        if (!File.Exists(_snapshotPath))
        {
            Console.WriteLine("[Persistence] No snapshot found — starting fresh.");
            return;
        }
        try
        {
            var json = await File.ReadAllTextAsync(_snapshotPath);
            var snapshot = JsonSerializer.Deserialize<BrokerSnapshot>(json);
            if (snapshot?.Topics is null) return;

            foreach (var ts in snapshot.Topics)
            {
                var topic = topics.GetOrAdd(ts.TopicId, id => new Topic(id));
                foreach (var groupId in ts.GroupIds)
                {
                    var mode = ts.GroupModes.TryGetValue(groupId, out var m) ? (DeliveryMode)m : DeliveryMode.Push;
                    var group = await topic.GetOrAddGroupAsync(groupId, mode);

                    // Restore offset from journal
                    var savedOffset = await ReadOffsetJournalAsync(groupId);
                    if (savedOffset >= 0) group.Offset = savedOffset;
                }
            }
            Console.WriteLine($"[Persistence] Restored {snapshot.Topics.Count} topic(s) from snapshot.");
        }
        catch (Exception ex) { Console.WriteLine($"[Persistence] Restore failed: {ex.Message}"); }
    }

    // ---- Offset Journal ----

    private string OffsetJournalPath(uint groupId) => Path.Combine(dataDir, $"cg_offset_{groupId}.bin");

    private async Task WriteOffsetJournalAsync(uint groupId, long offset)
    {
        var path = OffsetJournalPath(groupId);
        Directory.CreateDirectory(dataDir);
        var buf = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buf, offset);
        await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 8, true);
        await fs.WriteAsync(buf);
    }

    private async Task<long> ReadOffsetJournalAsync(uint groupId)
    {
        var path = OffsetJournalPath(groupId);
        if (!File.Exists(path)) return -1;
        var buf = new byte[8];
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 8, true);
        var read = await fs.ReadAsync(buf);
        return read == 8 ? System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(buf) : -1;
    }
}

// ---- JSON snapshot models ----

public sealed class BrokerSnapshot
{
    public List<TopicSnapshot> Topics { get; set; } = [];
}

public sealed class TopicSnapshot
{
    public uint           TopicId    { get; set; }
    public List<uint>     GroupIds   { get; set; } = [];
    public Dictionary<uint, byte> GroupModes { get; set; } = [];
}
