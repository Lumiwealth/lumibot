import { RevenueForecast } from "@/components/RevenueForecast";
import { prisma } from "@/lib/prisma";
import { serializeLead } from "@/lib/utils";

export const dynamic = "force-dynamic";

export default async function ForecastPage() {
  const leads = (await prisma.lead.findMany({ orderBy: [{ fit_score: "desc" }, { updated_at: "desc" }] })).map(serializeLead);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Revenue forecast</h1>
        <p className="mt-2 max-w-3xl text-slate-400">
          Estimate the pipeline needed to reach $10K-$20K/month from consulting, fractional, pilots, licensing, and
          advisory opportunities.
        </p>
      </div>
      <RevenueForecast leads={leads} />
    </div>
  );
}
