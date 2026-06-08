import Link from "next/link";
import { AlertCircle, CheckCircle2 } from "lucide-react";
import { PriorityBadge } from "@/components/PriorityBadge";
import { StageBadge } from "@/components/StageBadge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LeadRecord } from "@/lib/types";
import { formatDate } from "@/lib/utils";

export function FollowUpList({ leads, title = "Follow-ups due" }: { leads: LeadRecord[]; title?: string }) {
  const now = new Date();

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>Overdue and upcoming manual follow-ups.</CardDescription>
      </CardHeader>
      <CardContent>
        {leads.length === 0 ? (
          <div className="flex items-center gap-3 rounded-xl border border-slate-800 bg-slate-950/50 p-4 text-sm text-slate-300">
            <CheckCircle2 className="h-5 w-5 text-teal-300" />
            No follow-ups are due right now.
          </div>
        ) : (
          <div className="space-y-3">
            {leads.map((lead) => {
              const dueDate = lead.follow_up_date ? new Date(lead.follow_up_date) : null;
              const overdue = dueDate ? dueDate < now : false;

              return (
                <Link
                  key={lead.id}
                  href={`/leads/${lead.id}`}
                  className="block rounded-xl border border-slate-800 bg-slate-950/50 p-4 hover:bg-slate-900"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="flex items-center gap-2">
                        {overdue ? <AlertCircle className="h-4 w-4 text-amber-300" /> : null}
                        <span className="font-semibold text-white">{lead.company_name}</span>
                      </div>
                      <div className="mt-1 text-sm text-slate-400">{lead.next_action || "Send a concise check-in."}</div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <PriorityBadge priority={lead.priority_level} />
                      <StageBadge stage={lead.stage} />
                    </div>
                  </div>
                  <div className="mt-3 text-xs text-slate-500">
                    Due {formatDate(lead.follow_up_date)} · {lead.suggested_offer || "Offer TBD"}
                  </div>
                </Link>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
