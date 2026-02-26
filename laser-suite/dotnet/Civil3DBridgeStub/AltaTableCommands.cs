using System.Threading.Tasks;
using Autodesk.AutoCAD.ApplicationServices;
using Autodesk.AutoCAD.DatabaseServices;
using Autodesk.AutoCAD.EditorInput;
using Autodesk.AutoCAD.Geometry;
using Autodesk.AutoCAD.Runtime;

namespace Civil3DBridgeStub;

public static class AltaTableCommands
{
    [CommandMethod("LASER_ITEM20_TABLE")]
    public static async Task ProcessNextAsync()
    {
        if (!BridgeQueue.TryDequeue(out var payload))
        {
            return;
        }

        var doc = Application.DocumentManager.MdiActiveDocument;
        if (doc == null)
        {
            return;
        }

        var db = doc.Database;
        var ed = doc.Editor;

        PromptPointResult prompt = ed.GetPoint("\nSelect insertion point for Item 20 table: ");
        if (prompt.Status != PromptStatus.OK)
        {
            return;
        }

        using (doc.LockDocument())
        using (Transaction tr = db.TransactionManager.StartTransaction())
        {
            var table = new Table
            {
                TableStyle = db.Tablestyle,
                Position = prompt.Value,
                NumRows = payload.Item20Rows.Count + 1,
                NumColumns = 4,
            };
            table.SetRowHeight(3.0);
            table.SetColumnWidth(20.0);

            table.Cells[0, 0].TextString = "ID";
            table.Cells[0, 1].TextString = "Condition";
            table.Cells[0, 2].TextString = "Location";
            table.Cells[0, 3].TextString = "Magnitude";

            for (int i = 0; i < payload.Item20Rows.Count; i++)
            {
                var row = payload.Item20Rows[i];
                int r = i + 1;
                table.Cells[r, 0].TextString = row.ItemId;
                table.Cells[r, 1].TextString = row.ConditionType;
                table.Cells[r, 2].TextString = row.LocationReference;
                table.Cells[r, 3].TextString = $"{row.Magnitude:0.###} {row.Units}";
            }

            var btr = (BlockTableRecord)tr.GetObject(db.CurrentSpaceId, OpenMode.ForWrite);
            btr.AppendEntity(table);
            tr.AddNewlyCreatedDBObject(table, true);
            tr.Commit();
        }

        ComplianceNotifier.NotifyIfNeeded(payload);
        await Task.CompletedTask;
    }
}
