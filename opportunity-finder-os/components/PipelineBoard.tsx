import Link from "next/link";
import { StageBadge } from "@/components/StageBadge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { PIPELINE_STAGES } from "@/lib/constants";
import type { LeadRecord } from "@/lib/types";

export function PipelineBoard({ leads }: { leads: LeadRecord[] }) {
  const stages = PIPELINE_STAGES.map((stage) => ({
    stage,
    leads: leads.filter((lead) => lead.stage === stage).slice(0, 4),
  })).filter((group) => group.leads.length > 0);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pipeline board</CardTitle>
        <CardDescription>Latest opportunities grouped by stage.</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {stages.map((group) => (
            <div key={group.stage} className="rounded-xl border border-slate-800 bg-slate-950/50 p-3">
              <div className="mb-3 flex items-center justify-between">
                <StageBadge stage={group.stage} />
                <span className="text-xs text-slate-500">{group.leads.length}</span>
              </div>
              <div className="space-y-2">
                {group.leads.map((lead) => (
                  <Link
                    href={`/leads/${lead.id}`}
                    key={lead.id}
                    className="block rounded-lg bg-slate-900 p-3 hover:bg-slate-800"
                  >
                    <div className="font-medium text-white">{lead.company_name}</div>
                    <div className="mt-1 text-xs text-slate-400">{lead.suggested_offer || "Offer TBD"}</div>
                    <div className="mt-2 text-xs text-teal-200">Score {lead.fit_score}</div>
                  </Link>
                ))}
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
