import { PriorityBadge } from "@/components/PriorityBadge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LeadRecord } from "@/lib/types";

export function LeadScoreCard({ lead }: { lead: LeadRecord }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Fit score</CardTitle>
        <CardDescription>Weighted against budget, fit, LCS relevance, urgency, and access.</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex items-end justify-between gap-6">
          <div>
            <div className="text-6xl font-bold tracking-tight text-white">{lead.fit_score}</div>
            <div className="mt-2">
              <PriorityBadge priority={lead.priority_level} />
            </div>
          </div>
          <div className="h-28 w-28 rounded-full border-8 border-slate-800 p-2">
            <div className="flex h-full w-full items-center justify-center rounded-full bg-teal-400/10 text-sm font-semibold text-teal-100">
              {lead.fit_score >= 85 ? "Contact today" : lead.fit_score >= 70 ? "Strong" : "Test"}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
