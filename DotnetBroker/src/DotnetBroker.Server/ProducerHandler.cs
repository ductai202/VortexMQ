using System.Net.Sockets;
using DotnetBroker.Core.Models;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Server;

/// <summary>
/// Reads PCM messages from a single producer's TCP stream and pushes them into the topic queue.
/// Runs as a background task per producer connection.
/// </summary>
public sealed class ProducerHandler(Topic topic, ushort producerPort, NetworkStream stream, TcpClient client)
{
    public async Task RunAsync(CancellationToken ct)
    {
        Console.WriteLine($"[ProducerHandler] Listening for PCM from producer port={producerPort} topic={topic.TopicId}");
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var (msgType, payload) = await stream.ReadMessageAsync(ct);
                if (msgType != MessageType.Pcm)
                {
                    Console.WriteLine($"[ProducerHandler] Unexpected message type {msgType} from producer {producerPort}");
                    continue;
                }

                var pcm = ProduceConsumePayload.Decode(payload);
                try
                {
                    topic.Queue.PushBack(pcm);
                    Console.WriteLine($"[ProducerHandler] Queued msg from port={producerPort} ts={pcm.Timestamp} len={pcm.Message.Length} q_size={topic.Queue.Count}");
                }
                catch (InvalidOperationException ex)
                {
                    // Ring buffer full — send error ACK
                    Console.WriteLine($"[ProducerHandler] Ring buffer FULL for topic {topic.TopicId}: {ex.Message}");
                    await stream.WriteAckAsync(MessageType.R_P_Reg, 1, ct); // error=1
                    continue;
                }

                // Acknowledge the PCM
                await stream.WriteAckAsync(MessageType.R_Pcm, 0, ct);
            }
        }
        catch (EndOfStreamException) { Console.WriteLine($"[ProducerHandler] Producer port={producerPort} disconnected."); }
        catch (IOException ex) when (ex.InnerException is SocketException) { Console.WriteLine($"[ProducerHandler] Socket error: {ex.Message}"); }
        catch (OperationCanceledException) { }
        finally { client.Dispose(); }
    }
}
