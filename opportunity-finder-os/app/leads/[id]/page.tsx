import Link from "next/link";
import { notFound } from "next/navigation";
import { updateLeadStage } from "@/app/leads/actions";
import { LeadDetailPanel } from "@/components/LeadDetailPanel";
import { LeadScoreCard } from "@/components/LeadScoreCard";
import { OutreachGenerator } from "@/components/OutreachGenerator";
import { ScoreBreakdown } from "@/components/ScoreBreakdown";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { PIPELINE_STAGES } from "@/lib/constants";
import { prisma } from "@/lib/prisma";
import { serializeLead, formatDateInput } from "@/lib/utils";

export default async function LeadDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const rawLead = await prisma.lead.findUnique({ where: { id } });

  if (!rawLead) notFound();

  const lead = serializeLead(rawLead);
  const updateAction = updateLeadStage.bind(null, lead.id);

  return (
    <div className="space-y-6">
      <div className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
        <div>
          <Link href="/leads" className="text-sm font-medium text-teal-200 hover:text-teal-100">
            ← Back to leads
          </Link>
          <h1 className="mt-2 text-3xl font-bold tracking-tight text-white">{lead.company_name}</h1>
          <p className="mt-2 text-slate-400">
            Pitch: <span className="text-slate-200">{lead.suggested_offer || "Offer TBD"}</span>
          </p>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[1fr_380px]">
        <div className="space-y-6">
          <LeadDetailPanel lead={lead} />
          <Card>
            <CardHeader>
              <CardTitle>What should Rico say?</CardTitle>
              <CardDescription>Deterministic templates using this lead&apos;s fields and LCS Engine proof-of-work.</CardDescription>
            </CardHeader>
            <CardContent>
              <OutreachGenerator lead={lead} />
            </CardContent>
          </Card>
        </div>

        <div className="space-y-6">
          <LeadScoreCard lead={lead} />
          <Card>
            <CardHeader>
              <CardTitle>Score breakdown</CardTitle>
              <CardDescription>Weighted factors on a 1-5 scale.</CardDescription>
            </CardHeader>
            <CardContent>
              <ScoreBreakdown lead={lead} />
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Update stage</CardTitle>
              <CardDescription>After manual outreach, update the pipeline and next follow-up.</CardDescription>
            </CardHeader>
            <CardContent>
              <form action={updateAction} className="space-y-4">
                <div>
                  <Label htmlFor="stage">Stage</Label>
                  <select
                    id="stage"
                    name="stage"
                    defaultValue={lead.stage}
                    className="h-10 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 text-sm text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-400"
                  >
                    {PIPELINE_STAGES.map((stage) => (
                      <option key={stage} value={stage}>
                        {stage}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <Label htmlFor="follow_up_date">Follow-up date</Label>
                  <input
                    id="follow_up_date"
                    name="follow_up_date"
                    type="date"
                    defaultValue={formatDateInput(lead.follow_up_date)}
                    className="h-10 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 text-sm text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-400"
                  />
                </div>
                <div>
                  <Label htmlFor="next_action">Next action</Label>
                  <Textarea id="next_action" name="next_action" defaultValue={lead.next_action || ""} />
                </div>
                <div>
                  <Label htmlFor="notes">Notes</Label>
                  <Textarea id="notes" name="notes" defaultValue={lead.notes || ""} />
                </div>
                <Button type="submit" className="w-full">
                  Save pipeline update
                </Button>
              </form>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
