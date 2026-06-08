import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

const weights = {
  ability_to_pay: 0.2,
  fit_with_my_background: 0.15,
  need_for_ai_product_help: 0.15,
  relevance_to_lcs: 0.15,
  accessibility_of_decision_maker: 0.1,
  warm_intro_strength: 0.1,
  urgency: 0.1,
  remote_or_fractional_fit: 0.05,
} as const;

type ScoredLead = {
  ability_to_pay: number;
  fit_with_my_background: number;
  need_for_ai_product_help: number;
  relevance_to_lcs: number;
  accessibility_of_decision_maker: number;
  warm_intro_strength: number;
  urgency: number;
  remote_or_fractional_fit: number;
  category: string;
};

function score(lead: ScoredLead) {
  return Math.round(
    Object.entries(weights).reduce((sum, [key, weight]) => {
      const value = lead[key as keyof typeof weights];
      return sum + (value / 5) * weight;
    }, 0) * 100,
  );
}

function priority(fitScore: number) {
  if (fitScore >= 85) return "Must Contact";
  if (fitScore >= 70) return "Strong Lead";
  if (fitScore >= 55) return "Worth Testing";
  if (fitScore >= 40) return "Low Priority";
  return "Ignore for Now";
}

function offer(category: string, fitScore: number) {
  const normalized = category.toLowerCase();
  if (normalized.includes("family office")) return fitScore >= 80 ? "LCS Licensing Conversation" : "Family Office Innovation Scout";
  if (normalized.includes("ria") || normalized.includes("wealth")) return "Investment Education Platform Consultant";
  if (normalized.includes("fintech") || normalized.includes("ai startup")) return fitScore >= 75 ? "Fractional AI Product Lead" : "AI Product Strategy Consultant";
  if (normalized.includes("hbcu") || normalized.includes("workforce")) return fitScore >= 70 ? "LCS Institutional Pilot" : "Calibration/Prediction Lab Workshop";
  if (normalized.includes("credit union")) return fitScore >= 70 ? "LCS Institutional Pilot" : "AI Financial Literacy Consultant";
  if (normalized.includes("edtech")) return fitScore >= 70 ? "Decision Intelligence Consultant" : "Product Strategy Sprint";
  if (normalized.includes("prediction")) return "Decision Intelligence Consultant";
  if (normalized.includes("venture") || normalized.includes("accelerator") || normalized.includes("studio")) return "Startup Operator-in-Residence";
  return "AI Workflow Audit";
}

function revenue(category: string, fitScore: number) {
  const normalized = category.toLowerCase();
  if (normalized.includes("hbcu") || normalized.includes("credit union") || normalized.includes("education")) return "$15K-$25K institutional pilot/license";
  if (normalized.includes("family office") || normalized.includes("wealth") || normalized.includes("fintech")) return fitScore >= 80 ? "$10K-$20K/month retainer" : "$5K-$10K/month retainer";
  if (normalized.includes("startup") || normalized.includes("studio") || normalized.includes("accelerator")) return fitScore >= 75 ? "$5K-$10K/month retainer" : "Advisor/equity opportunity";
  return fitScore >= 70 ? "$5K-$10K/month retainer" : "$2K-$5K project";
}

function daysFromNow(days: number) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date;
}

const baseLeads = [
  {
    company_name: "Northstar Family Capital",
    category: "Family Office",
    website: "https://example.com/northstar",
    location: "New York, NY",
    contact_name: "Jordan Ellis",
    contact_title: "Managing Director",
    contact_email: "contact@example.com",
    linkedin_url: "https://linkedin.com/in/example",
    warm_intro_source: "Ari from investor dinner",
    notes: "Multi-generational family office exploring AI education for next-gen family members and internal investment memos.",
    estimated_budget: "$10K-$20K/month",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 5,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 5,
    institutional_education_fit: 3,
    accessibility_of_decision_maker: 4,
    warm_intro_strength: 4,
    remote_or_fractional_fit: 5,
    stage: "Researched",
    next_action: "Ask Ari for a warm intro and mention LCS next-gen education angle.",
    follow_up_date: daysFromNow(2),
    personalized_angle: "They are actively discussing AI tools for investment education and next-gen family learning.",
    objection_risk: "May prefer established consultants.",
    confidence_level: "High",
  },
  {
    company_name: "HarborPath Wealth Advisors",
    category: "RIA / Wealth Manager",
    website: "https://example.com/harborpath",
    location: "Charlotte, NC",
    contact_name: "Maya Chen",
    contact_title: "Chief Growth Officer",
    contact_email: "contact@example.com",
    notes: "RIA wants better client education around macro uncertainty and long-term portfolio behavior.",
    estimated_budget: "$7.5K/month",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 5,
    institutional_education_fit: 3,
    accessibility_of_decision_maker: 3,
    warm_intro_strength: 2,
    remote_or_fractional_fit: 5,
    stage: "Scored",
    next_action: "Draft client education platform email.",
    follow_up_date: daysFromNow(4),
    personalized_angle: "Their client education content could become an interactive decision-quality learning loop.",
    objection_risk: "Compliance review may slow down pilot.",
    confidence_level: "High",
  },
  {
    company_name: "LedgerLift",
    category: "Fintech Startup",
    website: "https://example.com/ledgerlift",
    location: "Remote",
    contact_name: "Sam Patel",
    contact_title: "Founder",
    contact_email: "contact@example.com",
    warm_intro_source: "Fintech Slack group",
    notes: "Seed-stage fintech with onboarding friction and vague AI roadmap.",
    estimated_budget: "$5K-$10K/month",
    urgency: 5,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 5,
    need_for_ai_product_help: 5,
    relevance_to_lcs: 4,
    relevance_to_decision_intelligence: 4,
    family_office_or_wealth_fit: 2,
    institutional_education_fit: 2,
    accessibility_of_decision_maker: 5,
    warm_intro_strength: 3,
    remote_or_fractional_fit: 5,
    stage: "Outreach Drafted",
    next_action: "Send founder-to-founder product sprint pitch.",
    follow_up_date: daysFromNow(1),
    personalized_angle: "They need a shipped AI workflow more than another strategy document.",
    objection_risk: "Budget sensitivity.",
    confidence_level: "High",
  },
  {
    company_name: "SignalForge AI",
    category: "AI Startup",
    website: "https://example.com/signalforge",
    location: "San Francisco, CA",
    contact_name: "Elena Brooks",
    contact_title: "CEO",
    contact_email: "contact@example.com",
    notes: "AI startup building analyst productivity tools for boutique investment teams.",
    estimated_budget: "$10K/month",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 4,
    need_for_ai_product_help: 5,
    relevance_to_lcs: 4,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 4,
    institutional_education_fit: 2,
    accessibility_of_decision_maker: 3,
    warm_intro_strength: 1,
    remote_or_fractional_fit: 5,
    stage: "Found",
    next_action: "Research product and identify AI workflow wedge.",
    follow_up_date: daysFromNow(6),
    personalized_angle: "Their analyst workflow overlaps with prediction, confidence, and decision-quality measurement.",
    objection_risk: "May already have deep AI team.",
    confidence_level: "Medium",
  },
  {
    company_name: "Community First Credit Union",
    category: "Credit Union",
    website: "https://example.com/communityfirst",
    location: "Jacksonville, FL",
    contact_name: "Andre Williams",
    contact_title: "VP Member Experience",
    contact_email: "contact@example.com",
    notes: "Credit union wants younger member engagement and practical financial literacy programming.",
    estimated_budget: "$20K pilot",
    urgency: 3,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 3,
    institutional_education_fit: 5,
    accessibility_of_decision_maker: 3,
    warm_intro_strength: 2,
    remote_or_fractional_fit: 4,
    stage: "Sent",
    next_action: "Follow up with institutional pilot framing.",
    last_contacted_date: daysFromNow(-8),
    follow_up_date: daysFromNow(0),
    personalized_angle: "Member education could use prediction and calibration instead of generic budgeting modules.",
    objection_risk: "Procurement cycle.",
    confidence_level: "Medium",
  },
  {
    company_name: "Atlanta Future Skills Collaborative",
    category: "HBCU / Workforce Program",
    website: "https://example.com/futureskills",
    location: "Atlanta, GA",
    contact_name: "Dr. Tasha Morgan",
    contact_title: "Program Director",
    contact_email: "contact@example.com",
    warm_intro_source: "Local entrepreneurship mentor",
    notes: "Workforce program wants AI and financial decision-making curriculum for adult learners.",
    estimated_budget: "$15K pilot",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 3,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 1,
    institutional_education_fit: 5,
    accessibility_of_decision_maker: 4,
    warm_intro_strength: 4,
    remote_or_fractional_fit: 4,
    stage: "Call Booked",
    next_action: "Prepare discovery call agenda and LCS demo path.",
    follow_up_date: daysFromNow(3),
    personalized_angle: "Their learners need applied AI and decision-making under uncertainty, not generic prompt engineering.",
    objection_risk: "Grant timing.",
    confidence_level: "High",
  },
  {
    company_name: "LearnVest Labs",
    category: "Edtech Company",
    website: "https://example.com/learnvestlabs",
    location: "Remote",
    contact_name: "Priya Shah",
    contact_title: "Head of Product",
    contact_email: "contact@example.com",
    notes: "Edtech company is adding financial literacy products for employer benefits partners.",
    estimated_budget: "$5K-$10K/month",
    urgency: 3,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 2,
    institutional_education_fit: 5,
    accessibility_of_decision_maker: 3,
    warm_intro_strength: 1,
    remote_or_fractional_fit: 5,
    stage: "Found",
    next_action: "Find product leader intro path.",
    personalized_angle: "Their financial literacy roadmap could become an interactive prediction and calibration product.",
    objection_risk: "May want a pure curriculum vendor.",
    confidence_level: "Medium",
  },
  {
    company_name: "ForgeHouse Ventures",
    category: "Venture Studio",
    website: "https://example.com/forgehouse",
    location: "Austin, TX",
    contact_name: "Marcus Reed",
    contact_title: "Partner",
    contact_email: "contact@example.com",
    notes: "Venture studio launches fintech and B2B SaaS concepts; needs operator help validating AI workflows.",
    estimated_budget: "$7.5K/month",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 5,
    need_for_ai_product_help: 5,
    relevance_to_lcs: 3,
    relevance_to_decision_intelligence: 4,
    family_office_or_wealth_fit: 2,
    institutional_education_fit: 2,
    accessibility_of_decision_maker: 4,
    warm_intro_strength: 2,
    remote_or_fractional_fit: 5,
    stage: "Researched",
    next_action: "Pitch operator-in-residence sprint.",
    follow_up_date: daysFromNow(5),
    personalized_angle: "They need founder-level product ambiguity reduction across AI concepts.",
    objection_risk: "Equity-heavy compensation.",
    confidence_level: "Medium",
  },
  {
    company_name: "LaunchBridge Accelerator",
    category: "Accelerator",
    website: "https://example.com/launchbridge",
    location: "Chicago, IL",
    contact_name: "Nina Alvarez",
    contact_title: "Managing Director",
    contact_email: "contact@example.com",
    notes: "Accelerator supports fintech and AI founders; may need workshops and product office hours.",
    estimated_budget: "$3K workshop",
    urgency: 3,
    remote_friendly: true,
    ability_to_pay: 3,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 3,
    relevance_to_decision_intelligence: 4,
    family_office_or_wealth_fit: 2,
    institutional_education_fit: 4,
    accessibility_of_decision_maker: 4,
    warm_intro_strength: 1,
    remote_or_fractional_fit: 5,
    stage: "Nurture",
    next_action: "Send workshop idea after next cohort announcement.",
    personalized_angle: "Their founders would benefit from seeing how a solo founder shipped LCS end-to-end.",
    objection_risk: "Small program budget.",
    confidence_level: "Medium",
  },
  {
    company_name: "ProbCast Markets",
    category: "Prediction Market Company",
    website: "https://example.com/probcast",
    location: "Remote",
    contact_name: "Owen Wright",
    contact_title: "Product Lead",
    contact_email: "contact@example.com",
    notes: "Prediction market company wants better user education around calibration and confidence.",
    estimated_budget: "$5K-$10K/month",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 4,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 2,
    institutional_education_fit: 3,
    accessibility_of_decision_maker: 3,
    warm_intro_strength: 2,
    remote_or_fractional_fit: 5,
    stage: "Proposal Sent",
    next_action: "Follow up on calibration workshop proposal.",
    last_contacted_date: daysFromNow(-6),
    follow_up_date: daysFromNow(1),
    personalized_angle: "Their users already think in probabilities, but need education that improves calibration.",
    objection_risk: "May prefer in-house content.",
    confidence_level: "High",
  },
  {
    company_name: "Atlas Portfolio",
    category: "Investment Platform",
    website: "https://example.com/atlasportfolio",
    location: "Boston, MA",
    contact_name: "Grace Kim",
    contact_title: "Director of Product",
    contact_email: "contact@example.com",
    notes: "Investment platform exploring educational AI companions and portfolio learning loops.",
    estimated_budget: "$10K/month",
    urgency: 4,
    remote_friendly: true,
    ability_to_pay: 5,
    fit_with_my_background: 5,
    need_for_ai_product_help: 4,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 4,
    institutional_education_fit: 4,
    accessibility_of_decision_maker: 3,
    warm_intro_strength: 1,
    remote_or_fractional_fit: 4,
    stage: "Scored",
    next_action: "Send AI financial education product note.",
    personalized_angle: "Their platform could close the loop between macro learning and portfolio decisions.",
    objection_risk: "Roadmap already full.",
    confidence_level: "High",
  },
  {
    company_name: "WealthOS Cloud",
    category: "Wealthtech Platform",
    website: "https://example.com/wealthos",
    location: "Miami, FL",
    contact_name: "Leo Ramirez",
    contact_title: "Co-founder",
    contact_email: "contact@example.com",
    warm_intro_source: "Former coworker",
    notes: "Wealthtech platform serving boutique advisors; interested in AI-powered client education.",
    estimated_budget: "$15K/month",
    urgency: 5,
    remote_friendly: true,
    ability_to_pay: 5,
    fit_with_my_background: 5,
    need_for_ai_product_help: 5,
    relevance_to_lcs: 5,
    relevance_to_decision_intelligence: 5,
    family_office_or_wealth_fit: 5,
    institutional_education_fit: 3,
    accessibility_of_decision_maker: 4,
    warm_intro_strength: 5,
    remote_or_fractional_fit: 5,
    stage: "Followed Up",
    next_action: "Ask for product strategy sprint call.",
    last_contacted_date: daysFromNow(-4),
    follow_up_date: daysFromNow(2),
    personalized_angle: "They could embed decision-intelligence education into advisor-client workflows.",
    objection_risk: "Needs clear first milestone.",
    confidence_level: "High",
  },
];

async function main() {
  await prisma.lead.deleteMany();

  for (const lead of baseLeads) {
    const fitScore = score(lead);
    await prisma.lead.create({
      data: {
        ...lead,
        fit_score: fitScore,
        priority_level: priority(fitScore),
        suggested_offer: offer(lead.category, fitScore),
        monthly_revenue_potential: revenue(lead.category, fitScore),
      },
    });
  }
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (error) => {
    console.error(error);
    await prisma.$disconnect();
    process.exit(1);
  });
