import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { REVENUE_ASSUMPTIONS } from "@/lib/constants";
import { buildRevenueForecast } from "@/lib/revenue";
import type { LeadRecord } from "@/lib/types";
import { formatCurrency } from "@/lib/utils";

export function RevenueForecast({ leads }: { leads: LeadRecord[] }) {
  const forecast = buildRevenueForecast(leads);

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-3">
        <ForecastMetric title="Conservative monthly" value={formatCurrency(forecast.conservativeMonthly)} />
        <ForecastMetric title="Realistic monthly" value={formatCurrency(forecast.realisticMonthly)} />
        <ForecastMetric title="Best-case monthly" value={formatCurrency(forecast.bestCaseMonthly)} />
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Conversation targets</CardTitle>
            <CardDescription>Based on current active conversation value and static funnel assumptions.</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-2">
            <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-5">
              <div className="text-sm text-slate-400">To reach $10K/month</div>
              <div className="mt-2 text-4xl font-bold text-white">{forecast.conversationsFor10k}</div>
              <div className="mt-1 text-xs text-slate-500">active conversations needed</div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-5">
              <div className="text-sm text-slate-400">To reach $20K/month</div>
              <div className="mt-2 text-4xl font-bold text-white">{forecast.conversationsFor20k}</div>
              <div className="mt-1 text-xs text-slate-500">active conversations needed</div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Default assumptions</CardTitle>
            <CardDescription>Editable later; static for this local-first MVP.</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-3 text-sm">
              <Assumption label="Cold leads to calls" value={`${REVENUE_ASSUMPTIONS.coldLeadToCallRate * 100}%`} />
              <Assumption label="Warm leads to calls" value={`${REVENUE_ASSUMPTIONS.warmLeadToCallRate * 100}%`} />
              <Assumption label="Calls to proposals" value={`${REVENUE_ASSUMPTIONS.callToProposalRate * 100}%`} />
              <Assumption label="Proposals close" value={`${REVENUE_ASSUMPTIONS.proposalCloseRate * 100}%`} />
              <Assumption label="Starter project" value={formatCurrency(REVENUE_ASSUMPTIONS.averageStarterProject)} />
              <Assumption label="Average retainer" value={formatCurrency(REVENUE_ASSUMPTIONS.averageRetainer)} />
              <Assumption label="Premium retainer" value={formatCurrency(REVENUE_ASSUMPTIONS.premiumRetainer)} />
              <Assumption label="Institutional pilot" value={formatCurrency(REVENUE_ASSUMPTIONS.institutionalPilot)} />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <ValueTable title="Pipeline value by category" rows={forecast.byCategory} />
        <ValueTable title="Pipeline value by stage" rows={forecast.byStage} />
      </div>
    </div>
  );
}

function ForecastMetric({ title, value }: { title: string; value: string }) {
  return (
    <Card>
      <CardHeader>
        <CardDescription>{title}</CardDescription>
        <CardTitle className="text-4xl">{value}</CardTitle>
      </CardHeader>
    </Card>
  );
}

function Assumption({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-950/50 p-3">
      <div className="text-slate-500">{label}</div>
      <div className="font-semibold text-white">{value}</div>
    </div>
  );
}

function ValueTable({ title, rows }: { title: string; rows: { name: string; value: number }[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead className="text-right">Value</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.name}>
                <TableCell>{row.name}</TableCell>
                <TableCell className="text-right font-semibold text-white">{formatCurrency(row.value)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}
