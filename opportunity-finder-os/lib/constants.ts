export const LEAD_CATEGORIES = [
  "Family Office",
  "RIA / Wealth Manager",
  "Fintech Startup",
  "AI Startup",
  "Credit Union",
  "HBCU / Workforce Program",
  "Financial Literacy Organization",
  "Venture Studio",
  "Accelerator",
  "Edtech Company",
  "Prediction Market Company",
  "Investment Platform",
  "Startup Studio",
  "Wealthtech Platform",
  "Economic Research / Market Intelligence",
] as const;

export const PIPELINE_STAGES = [
  "Found",
  "Researched",
  "Scored",
  "Outreach Drafted",
  "Sent",
  "Followed Up",
  "Call Booked",
  "Proposal Sent",
  "Won",
  "Lost",
  "Nurture",
] as const;

export const PRIORITY_LEVELS = [
  "Must Contact",
  "Strong Lead",
  "Worth Testing",
  "Low Priority",
  "Ignore for Now",
] as const;

export const OFFER_TYPES = [
  "Fractional AI Product Lead",
  "AI Product Strategy Consultant",
  "Family Office Innovation Scout",
  "Decision Intelligence Consultant",
  "Investment Education Platform Consultant",
  "AI Financial Literacy Consultant",
  "Startup Operator-in-Residence",
  "LCS Institutional Pilot",
  "LCS Licensing Conversation",
  "Calibration/Prediction Lab Workshop",
  "AI Workflow Audit",
  "Product Strategy Sprint",
] as const;

export const REVENUE_POTENTIALS = [
  "$2K-$5K project",
  "$5K-$10K/month retainer",
  "$10K-$20K/month retainer",
  "$15K-$25K institutional pilot/license",
  "Full-time role",
  "Advisor/equity opportunity",
] as const;

export const CONFIDENCE_LEVELS = ["Low", "Medium", "High"] as const;

export const SCORE_WEIGHTS = {
  ability_to_pay: 0.2,
  fit_with_my_background: 0.15,
  need_for_ai_product_help: 0.15,
  relevance_to_lcs: 0.15,
  accessibility_of_decision_maker: 0.1,
  warm_intro_strength: 0.1,
  urgency: 0.1,
  remote_or_fractional_fit: 0.05,
} as const;

export const SCORE_LABELS: Record<keyof typeof SCORE_WEIGHTS, string> = {
  ability_to_pay: "Ability to pay",
  fit_with_my_background: "Fit with Rico's background",
  need_for_ai_product_help: "Need for AI product help",
  relevance_to_lcs: "Relevance to LCS Engine",
  accessibility_of_decision_maker: "Decision-maker access",
  warm_intro_strength: "Warm intro strength",
  urgency: "Urgency",
  remote_or_fractional_fit: "Remote/fractional fit",
};

export const REVENUE_ASSUMPTIONS = {
  coldLeadToCallRate: 0.1,
  warmLeadToCallRate: 0.3,
  callToProposalRate: 0.25,
  proposalCloseRate: 0.3,
  averageStarterProject: 3000,
  averageRetainer: 7500,
  premiumRetainer: 15000,
  institutionalPilot: 20000,
};

export const POSITIONING = {
  default:
    "I help fintech, wealth, education, and family office teams turn AI from a vague strategy conversation into shipped decision-intelligence products.",
  familyOffice:
    "I help family offices evaluate and deploy practical AI tools around investment education, decision quality, portfolio learning, and next-gen financial literacy.",
  startup:
    "I help early-stage fintech and AI founders turn product ambiguity into shipped AI workflows, prototypes, and customer-facing features.",
  education:
    "I help institutions teach decision-making under uncertainty using prediction, calibration, and applied AI.",
};
