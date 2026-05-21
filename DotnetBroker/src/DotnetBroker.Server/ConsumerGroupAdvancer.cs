using System.Net.Sockets;
using DotnetBroker.Core.Models;
using DotnetBroker.Core.Protocol;
using DotnetBroker.Core.Queue;

namespace DotnetBroker.Server;

/// <summary>
/// Background task that advances a consumer group's offset and delivers messages to consumers.
/// Supports both Push mode (deliver immediately) and Pull mode (wait for C_RD ready signal).
/// Uses exponential backoff when no messages are available.
/// </summary>
public sealed class ConsumerGroupAdvancer(ConsumerGroup group, RingBufferQueue<ProduceConsumePayload> queue)
{
    private const int MinBackoffMs  = 10;
    private const int MaxBackoffMs  = 640;

    public async Task RunAsync(CancellationToken ct)
    {
        Console.WriteLine($"[Advancer-{group.GroupId}] Started (mode={group.Mode})");
        var backoffMs = MinBackoffMs;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // --- Get next message from queue ---
                if (!queue.TryPeek(group.Offset, out var message))
                {
                    // No message yet — exponential backoff
                    await Task.Delay(backoffMs, ct);
                    backoffMs = Math.Min(backoffMs * 2, MaxBackoffMs);
                    continue;
                }
                backoffMs = MinBackoffMs; // Reset backoff on success

                // --- Find a consumer to deliver to ---
                if (group.Mode == DeliveryMode.Pull)
                {
                    await DeliverPullAsync(message, ct);
                }
                else
                {
                    await DeliverPushAsync(message, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[Advancer-{group.GroupId}] Error: {ex.Message}");
                await Task.Delay(100, ct);
            }
        }
        Console.WriteLine($"[Advancer-{group.GroupId}] Stopped.");
    }

    // ---- Push mode ----
    private async Task DeliverPushAsync(ProduceConsumePayload message, CancellationToken ct)
    {
        var consumer = await group.TryGetConsumerAsync();
        if (consumer is null)
        {
            // No consumers available — wait a bit
            await Task.Delay(10, ct);
            return;
        }
        var (index, stream) = consumer.Value;

        if (!await SendAndAckAsync(index, stream, message, ct))
            return; // Consumer dead, try next iteration

        Interlocked.Increment(ref group.Offset);
    }

    // ---- Pull mode ----
    private async Task DeliverPullAsync(ProduceConsumePayload message, CancellationToken ct)
    {
        // In Pull mode, the consumer sends C_RD before receiving each message.
        // We read C_RD directly from the consumer stream before delivering.
        var consumer = await group.TryGetConsumerAsync();
        if (consumer is null)
        {
            await Task.Delay(10, ct);
            return;
        }
        var (index, stream) = consumer.Value;

        try
        {
            // Wait for C_RD from consumer
            var (rdType, _) = await stream.ReadMessageAsync(ct);
            if (rdType != MessageType.C_Rd)
            {
                Console.WriteLine($"[Advancer-{group.GroupId}] Expected C_RD but got {rdType}");
            }
            // Send R_C_RD ACK
            await stream.WriteAckAsync(MessageType.R_C_Rd, 0, ct);
        }
        catch (IOException ex)
        {
            Console.WriteLine($"[Advancer-{group.GroupId}] Consumer dead waiting for C_RD: {ex.Message}");
            await group.MarkDeadAsync(index);
            return;
        }

        if (!await SendAndAckAsync(index, stream, message, ct))
            return;

        Interlocked.Increment(ref group.Offset);
    }

    // ---- Shared delivery + ACK logic ----
    private async Task<bool> SendAndAckAsync(int index, NetworkStream stream, ProduceConsumePayload message, CancellationToken ct)
    {
        try
        {
            var encoded = message.Encode();
            await stream.WriteRawBytesAsync(MessageType.Pcm, encoded, ct);
            Console.WriteLine($"[Advancer-{group.GroupId}] Sent offset={group.Offset} to consumer index={index}");

            // Wait for R_PCM ACK from consumer
            var (ackType, _) = await stream.ReadMessageAsync(ct);
            if (ackType != MessageType.R_Pcm)
            {
                Console.WriteLine($"[Advancer-{group.GroupId}] Expected R_PCM but got {ackType}");
            }
            return true;
        }
        catch (IOException ex)
        {
            Console.WriteLine($"[Advancer-{group.GroupId}] Consumer index={index} dead: {ex.Message}");
            await group.MarkDeadAsync(index);
            return false;
        }
    }
}
