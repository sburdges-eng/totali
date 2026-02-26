using System.Linq;
using Autodesk.AutoCAD.ApplicationServices;

namespace Civil3DBridgeStub;

public static class ComplianceNotifier
{
    public static void NotifyIfNeeded(TopologicPayload payload)
    {
        bool hasFailure = payload.RppRows.Any(row => !row.Compliant);
        if (!hasFailure)
        {
            return;
        }

        var doc = Application.DocumentManager.MdiActiveDocument;
        if (doc == null)
        {
            return;
        }

        doc.Editor.WriteMessage("\nLASER compliance warning: one or more RPP pairs exceed allowable threshold.");
    }
}
