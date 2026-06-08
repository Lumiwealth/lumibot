import { FollowUpList } from "@/components/FollowUpList";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { prisma } from "@/lib/prisma";
import { serializeLead } from "@/lib/utils";

export const dynamic = "force-dynamic";

export default async function FollowUpsPage() {
  const leads = (await prisma.lead.findMany({ orderBy: [{ follow_up_date: "asc" }, { fit_score: "desc" }] })).map(serializeLead);
  const soon = new Date();
  soon.setDate(soon.getDate() + 7);
  const staleCutoff = new Date();
  staleCutoff.setDate(staleCutoff.getDate() - 10);

  const due = leads.filter(
    (lead) => lead.follow_up_date && new Date(lead.follow_up_date) <= soon && !["Won", "Lost"].includes(lead.stage),
  );
  const stale = leads.filter((lead) => {
    if (!lead.last_contacted_date || ["Won", "Lost", "Nurture"].includes(lead.stage)) return false;
    const last = new Date(lead.last_contacted_date);
    return last <= staleCutoff && !lead.follow_up_date;
  });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Follow-up tracker</h1>
        <p className="mt-2 max-w-3xl text-slate-400">
          Keep manual outreach moving by reviewing overdue, due-soon, and stale leads.
        </p>
      </div>

      <FollowUpList leads={due} title="Due soon or overdue" />

      <Card>
        <CardHeader>
          <CardTitle>Stale leads needing a next action</CardTitle>
          <CardDescription>Contacted at least 10 days ago with no follow-up date.</CardDescription>
        </CardHeader>
        <CardContent>
          <FollowUpList leads={stale} title="Stale leads" />
        </CardContent>
      </Card>
    </div>
  );
}
