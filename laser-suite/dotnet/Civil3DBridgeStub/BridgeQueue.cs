using System.Collections.Concurrent;
using Autodesk.AutoCAD.ApplicationServices;

namespace Civil3DBridgeStub;

public static class BridgeQueue
{
    private static readonly ConcurrentQueue<TopologicPayload> Queue = new();

    public static void Enqueue(TopologicPayload payload)
    {
        Queue.Enqueue(payload);
        Application.ExecuteInCommandContextAsync(async _ =>
        {
            await AltaTableCommands.ProcessNextAsync();
        }, null);
    }

    public static bool TryDequeue(out TopologicPayload payload) => Queue.TryDequeue(out payload!);
}
