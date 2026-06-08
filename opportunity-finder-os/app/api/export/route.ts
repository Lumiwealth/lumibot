import { prisma } from "@/lib/prisma";
import { leadsToCsv } from "@/lib/csv";

export async function GET() {
  const leads = await prisma.lead.findMany({
    orderBy: [{ fit_score: "desc" }, { company_name: "asc" }],
  });
  const csv = leadsToCsv(leads);

  return new Response(csv, {
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="opportunity-finder-leads.csv"`,
    },
  });
}
