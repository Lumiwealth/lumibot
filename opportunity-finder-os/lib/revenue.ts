import { REVENUE_ASSUMPTIONS } from "@/lib/constants";

export type RevenueLead = {
  category: string;
  stage: string;
  fit_score: number;
  priority_level: string;
  monthly_revenue_potential?: string | null;
  warm_intro_source?: string | null;
};

export function estimateLeadValue(lead: RevenueLead) {
  const potential = lead.monthly_revenue_potential || "";

  if (potential.includes("$10K-$20K")) return REVENUE_ASSUMPTIONS.premiumRetainer;
  if (potential.includes("$15K-$25K")) return REVENUE_ASSUMPTIONS.institutionalPilot;
  if (potential.includes("$5K-$10K")) return REVENUE_ASSUMPTIONS.averageRetainer;
  if (potential.includes("$2K-$5K")) return REVENUE_ASSUMPTIONS.averageStarterProject;
  if (potential.includes("Advisor")) return 2500;
  if (potential.includes("Full-time")) return 0;

  if (lead.fit_score >= 85) return REVENUE_ASSUMPTIONS.premiumRetainer;
  if (lead.fit_score >= 70) return REVENUE_ASSUMPTIONS.averageRetainer;
  return REVENUE_ASSUMPTIONS.averageStarterProject;
}

export function recommendRevenuePotential(category: string, score: number) {
  const normalized = category.toLowerCase();

  if (normalized.includes("hbcu") || normalized.includes("credit union") || normalized.includes("education")) {
    return "$15K-$25K institutional pilot/license";
  }

  if (normalized.includes("family office") || normalized.includes("wealth") || normalized.includes("fintech")) {
    return score >= 80 ? "$10K-$20K/month retainer" : "$5K-$10K/month retainer";
  }

  if (normalized.includes("startup") || normalized.includes("studio") || normalized.includes("accelerator")) {
    return score >= 75 ? "$5K-$10K/month retainer" : "Advisor/equity opportunity";
  }

  return score >= 70 ? "$5K-$10K/month retainer" : "$2K-$5K project";
}

export function stageProbability(stage: string) {
  if (stage === "Won") return 1;
  if (stage === "Proposal Sent") return REVENUE_ASSUMPTIONS.proposalCloseRate;
  if (stage === "Call Booked") {
    return REVENUE_ASSUMPTIONS.callToProposalRate * REVENUE_ASSUMPTIONS.proposalCloseRate;
  }
  if (stage === "Sent" || stage === "Followed Up" || stage === "Outreach Drafted") {
    return (
      REVENUE_ASSUMPTIONS.coldLeadToCallRate *
      REVENUE_ASSUMPTIONS.callToProposalRate *
      REVENUE_ASSUMPTIONS.proposalCloseRate
    );
  }
  return 0.03;
}

export function buildRevenueForecast(leads: RevenueLead[]) {
  const openLeads = leads.filter((lead) => lead.stage !== "Lost" && lead.priority_level !== "Ignore for Now");
  const bestCaseMonthly = openLeads.reduce((sum, lead) => sum + estimateLeadValue(lead), 0);
  const realisticMonthly = openLeads.reduce(
    (sum, lead) => sum + estimateLeadValue(lead) * stageProbability(lead.stage),
    0,
  );
  const conservativeMonthly = realisticMonthly * 0.55;

  const activeConversations = openLeads.filter((lead) =>
    ["Sent", "Followed Up", "Call Booked", "Proposal Sent"].includes(lead.stage),
  ).length;
  const expectedValuePerConversation =
    activeConversations > 0 ? realisticMonthly / activeConversations : REVENUE_ASSUMPTIONS.averageRetainer * 0.075;

  const conversationsFor10k = Math.ceil(10000 / Math.max(expectedValuePerConversation, 1));
  const conversationsFor20k = Math.ceil(20000 / Math.max(expectedValuePerConversation, 1));

  return {
    bestCaseMonthly,
    realisticMonthly,
    conservativeMonthly,
    activeConversations,
    conversationsFor10k,
    conversationsFor20k,
    byCategory: groupRevenue(openLeads, "category"),
    byStage: groupRevenue(openLeads, "stage"),
  };
}

function groupRevenue(leads: RevenueLead[], key: "category" | "stage") {
  const groups = new Map<string, number>();

  for (const lead of leads) {
    groups.set(lead[key], (groups.get(lead[key]) || 0) + estimateLeadValue(lead));
  }

  return Array.from(groups.entries())
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
}
