import { POSITIONING } from "@/lib/constants";

export type OutreachLead = {
  company_name: string;
  category: string;
  contact_name?: string | null;
  contact_title?: string | null;
  warm_intro_source?: string | null;
  notes?: string | null;
  suggested_offer?: string | null;
  personalized_angle?: string | null;
};

export function getRecommendedOffer(category: string, fitScore: number) {
  const normalized = category.toLowerCase();

  if (normalized.includes("family office")) {
    return fitScore >= 80 ? "LCS Licensing Conversation" : "Family Office Innovation Scout";
  }

  if (normalized.includes("ria") || normalized.includes("wealth")) {
    return fitScore >= 75
      ? "Investment Education Platform Consultant"
      : "AI Financial Literacy Consultant";
  }

  if (normalized.includes("fintech")) {
    return fitScore >= 75 ? "Fractional AI Product Lead" : "AI Product Strategy Consultant";
  }

  if (normalized.includes("ai startup")) {
    return fitScore >= 75 ? "Fractional AI Product Lead" : "AI Product Strategy Consultant";
  }

  if (normalized.includes("hbcu") || normalized.includes("workforce")) {
    return fitScore >= 70 ? "LCS Institutional Pilot" : "Calibration/Prediction Lab Workshop";
  }

  if (normalized.includes("credit union")) {
    return fitScore >= 70 ? "LCS Institutional Pilot" : "AI Financial Literacy Consultant";
  }

  if (normalized.includes("edtech")) {
    return fitScore >= 70 ? "Decision Intelligence Consultant" : "Product Strategy Sprint";
  }

  if (normalized.includes("prediction")) {
    return fitScore >= 70
      ? "Decision Intelligence Consultant"
      : "Calibration/Prediction Lab Workshop";
  }

  if (
    normalized.includes("venture studio") ||
    normalized.includes("accelerator") ||
    normalized.includes("startup studio")
  ) {
    return fitScore >= 70 ? "Startup Operator-in-Residence" : "Product Strategy Sprint";
  }

  return fitScore >= 75 ? "AI Product Strategy Consultant" : "AI Workflow Audit";
}

export function getPositioningLine(category: string) {
  const normalized = category.toLowerCase();

  if (normalized.includes("family office")) return POSITIONING.familyOffice;
  if (normalized.includes("hbcu") || normalized.includes("workforce") || normalized.includes("education")) {
    return POSITIONING.education;
  }
  if (normalized.includes("startup") || normalized.includes("studio") || normalized.includes("accelerator")) {
    return POSITIONING.startup;
  }

  return POSITIONING.default;
}

function greeting(lead: OutreachLead) {
  return lead.contact_name ? `Hi ${lead.contact_name.split(" ")[0]},` : "Hi there,";
}

function angle(lead: OutreachLead) {
  return (
    lead.personalized_angle ||
    lead.notes ||
    `I noticed ${lead.company_name} sits at the intersection of ${lead.category.toLowerCase()} and customer education.`
  );
}

function offer(lead: OutreachLead) {
  return lead.suggested_offer || getRecommendedOffer(lead.category, 70);
}

export function generateOutreach(lead: OutreachLead) {
  const positioning = getPositioningLine(lead.category);
  const selectedOffer = offer(lead);
  const personalAngle = angle(lead);
  const introSource = lead.warm_intro_source || "a mutual contact";

  return {
    coldEmail: {
      title: "Cold email",
      body: `${greeting(lead)}

${personalAngle}

${positioning} I independently built and deployed LCS Engine, a live decision-intelligence platform for investing education with a Next.js frontend, FastAPI backend, billing, analytics, AI tutoring, prediction labs, paper trading, and 135 backend tests.

I think there may be a useful conversation around ${selectedOffer.toLowerCase()} for ${lead.company_name}. If helpful, I can share a concise view of where practical AI product work could create momentum without turning into an open-ended strategy project.

Would it be worth a 20-minute conversation next week?`,
    },
    warmIntroRequest: {
      title: "Warm intro request",
      body: `Hi ${introSource},

Would you be open to introducing me to ${lead.contact_name || `someone at ${lead.company_name}`}?

I am reaching out because ${personalAngle.toLowerCase()} My angle is specific: ${positioning}

The relevant proof point is that I independently shipped LCS Engine, a working decision-intelligence platform for investing education with AI tutoring, prediction calibration, paper trading, billing, analytics, and a full production stack.

A simple intro line could be: "Rico built and shipped a decision-intelligence product for investing education and may have useful ideas around ${selectedOffer.toLowerCase()}."`,
    },
    linkedInDm: {
      title: "LinkedIn DM",
      body: `${greeting(lead)} ${personalAngle}

I built and deployed LCS Engine, a live AI decision-intelligence platform for investing education. ${positioning}

Curious if ${selectedOffer.toLowerCase()} is relevant for ${lead.company_name} this quarter. Open to a short exchange?`,
    },
    followUpEmail: {
      title: "Follow-up email",
      body: `${greeting(lead)}

Wanted to follow up on my note. The short version: I have shipped the kind of AI product work many teams are still discussing in abstract terms.

For ${lead.company_name}, I would look for one practical wedge: ${selectedOffer.toLowerCase()} tied to decision quality, investor education, or customer-facing AI workflows.

Worth comparing notes for 20 minutes?`,
    },
    secondFollowUpEmail: {
      title: "Second follow-up email",
      body: `${greeting(lead)}

Closing the loop for now. If AI product, decision intelligence, or investment education becomes more active for ${lead.company_name}, I would be glad to be useful.

My lane is practical: turning a fuzzy AI opportunity into a shipped workflow, pilot, prototype, or customer-facing feature. I can also share what I learned building LCS Engine end-to-end.`,
    },
    proposalBlurb: {
      title: "Proposal blurb",
      body: `${selectedOffer}: a focused engagement to help ${lead.company_name} turn AI and decision-intelligence ideas into a practical product plan, prototype, pilot, or shipped workflow. Rico brings founder/operator proof-of-work from independently building LCS Engine: a deployed investing education platform with prediction labs, calibration scoring, AI tutoring, paper trading, billing, analytics, and production infrastructure.`,
    },
    discoveryCallAgenda: {
      title: "Discovery call agenda",
      body: `1. Current goals for AI, education, decision quality, or product innovation
2. Where ${lead.company_name} has customer, member, student, or investor friction today
3. What has already been tried and what constraints matter
4. Where LCS Engine proof-of-work maps to ${lead.company_name}'s needs
5. Best first offer: ${selectedOffer}
6. Decide whether the right next step is a pilot, sprint, workshop, audit, or no-fit referral`,
    },
    referralAsk: {
      title: "Referral ask",
      body: `If ${lead.company_name} is not the right fit, is there a founder, family office, wealth platform, credit union, or education leader you think should see LCS Engine or discuss practical AI product work? I am specifically looking for teams where decision intelligence, investor education, financial literacy, or shipped AI workflows matter now.`,
    },
    reactivationNote: {
      title: "Reactivation note",
      body: `${greeting(lead)}

It has been a while, and I wanted to reconnect with a sharper update. I have now shipped LCS Engine, a real decision-intelligence platform for investing education with AI tutoring, prediction labs, calibration scoring, paper trading, billing, analytics, and a production backend.

I am focused on helping fintech, wealth, education, and family office teams ship practical AI products. If ${selectedOffer.toLowerCase()} is relevant for ${lead.company_name}, I would enjoy comparing notes.`,
    },
  };
}
