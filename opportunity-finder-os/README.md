# Opportunity Finder OS

Opportunity Finder OS is a polished local-first sales pipeline app for finding, scoring, prioritizing, and tracking consulting, fractional, startup, institutional, and family office opportunities that could produce $10K-$20K/month.

It answers one daily question:

> Who should Rico contact today, why are they a fit, what should he pitch, and what should he say?

The app positions Rico as a founder/operator and AI product builder who independently built and deployed LCS Engine, a shipped decision-intelligence platform for investing education.

## What the app does

- Maintains a local lead database in SQLite through Prisma.
- Scores each opportunity from 0-100 using weighted 1-5 fit factors.
- Recommends a priority level, offer type, and likely revenue potential.
- Generates deterministic outreach templates with no AI APIs.
- Tracks pipeline stage, next action, last contact date, and follow-up date.
- Shows a SaaS-style dashboard with Recharts visualizations.
- Forecasts conservative, realistic, and best-case monthly pipeline value.
- Exports leads to CSV.

## Tech stack

- Next.js App Router
- TypeScript
- Tailwind CSS
- shadcn/ui-style components
- Prisma ORM
- SQLite for local development
- Recharts
- Zod

The app is local-first today and structured so it can later move to Vercel plus PostgreSQL.

## Install and run locally

```bash
cd opportunity-finder-os
npm install
npx prisma migrate dev
npx prisma db seed
npm run dev
```

Open `http://localhost:3000`.

## Prisma and SQLite setup

The local database is configured in `.env`:

```bash
DATABASE_URL="file:./dev.db"
```

Prisma schema:

```bash
prisma/schema.prisma
```

Seed data:

```bash
prisma/seed.ts
```

The seed creates 12 realistic placeholder leads across family office, RIA, fintech startup, AI startup, credit union, HBCU/workforce, edtech, venture studio, accelerator, prediction market, investment platform, and wealthtech categories. All contact emails use `contact@example.com`.

## Scoring system

Each scoring input uses a 1-5 scale. The app converts the weighted score to 0-100.

Weights:

- ability_to_pay: 20%
- fit_with_my_background: 15%
- need_for_ai_product_help: 15%
- relevance_to_lcs: 15%
- accessibility_of_decision_maker: 10%
- warm_intro_strength: 10%
- urgency: 10%
- remote_or_fractional_fit: 5%

Priority levels:

- 85-100: Must Contact
- 70-84: Strong Lead
- 55-69: Worth Testing
- 40-54: Low Priority
- Below 40: Ignore for Now

Scoring logic lives in `lib/scoring.ts`.

## Offer recommendations

Offer recommendations are deterministic and based on category plus fit score. Examples:

- Family office: Family Office Innovation Scout, AI Workflow Audit, or LCS Licensing Conversation
- RIA / wealth manager: AI Financial Literacy Consultant or Investment Education Platform Consultant
- Fintech startup: Fractional AI Product Lead or AI Product Strategy Consultant
- HBCU / workforce program: LCS Institutional Pilot or Calibration/Prediction Lab Workshop
- Credit union: AI Financial Literacy Consultant or LCS Institutional Pilot
- Edtech: Decision Intelligence Consultant or Product Strategy Sprint
- Prediction market company: Decision Intelligence Consultant or Calibration/Prediction Lab Workshop
- Venture studio / accelerator: Startup Operator-in-Residence or Product Strategy Sprint
- AI startup: AI Product Strategy Consultant or Fractional AI Product Lead

Offer logic lives in `lib/outreach.ts`.

## How to add leads

1. Go to `/leads/new`.
2. Add company, category, contact info, notes, estimated budget, and a personalized angle.
3. Rate each fit factor from 1-5.
4. Save the lead.
5. The app calculates fit score, priority, suggested offer, and likely revenue potential.

## CSV export

Use the "Export CSV" button on the dashboard or lead database page.

CSV export endpoint:

```bash
/api/export
```

Sample CSV:

```bash
data/sample_leads.csv
```

TODO: CSV import is not implemented in this MVP. The recommended next version is an import screen that validates rows with Zod, previews errors, and creates leads through Prisma.

## Daily workflow

1. Add or import new leads.
2. Score them.
3. Review top 10 opportunities.
4. Generate outreach for 3-5 leads.
5. Send messages manually.
6. Update stages.
7. Review follow-ups due.
8. Track likely monthly revenue.

## Weekly workflow

1. Add 20 new leads.
2. Contact 10 high-fit leads.
3. Follow up with all stale leads.
4. Book 2-3 discovery calls.
5. Send 1-2 proposals.
6. Review revenue forecast.
7. Improve outreach based on replies.

## Positioning used in outreach

Default:

> I help fintech, wealth, education, and family office teams turn AI from a vague strategy conversation into shipped decision-intelligence products.

Family offices:

> I help family offices evaluate and deploy practical AI tools around investment education, decision quality, portfolio learning, and next-gen financial literacy.

Startups:

> I help early-stage fintech and AI founders turn product ambiguity into shipped AI workflows, prototypes, and customer-facing features.

Education/workforce:

> I help institutions teach decision-making under uncertainty using prediction, calibration, and applied AI.

Outreach generation lives in `lib/outreach.ts`.

## Revenue forecast assumptions

- 10% of cold leads become calls.
- 30% of warm leads become calls.
- 25% of calls become proposals.
- 30% of proposals close.
- Average starter project: $3,000.
- Average retainer: $7,500/month.
- Premium retainer: $15,000/month.
- Institutional pilot/license: $20,000.

Forecast logic lives in `lib/revenue.ts`.

## Routes

- `/` - Dashboard
- `/leads` - Lead list with filters, search, sorting, badges, and score display
- `/leads/new` - Add lead form
- `/leads/[id]` - Lead detail, score breakdown, outreach drafts, notes, and stage updates
- `/outreach` - Outreach generator
- `/forecast` - Revenue forecast
- `/followups` - Follow-up tracker
- `/settings` - Static scoring and revenue assumptions

## Future enhancements

- CSV import with validation and preview.
- Editable scoring and revenue assumptions.
- Lead edit page for full lead profile updates.
- Activity timeline.
- Vercel deployment.
- PostgreSQL migration.
- Authentication.
- Browser automation.
- Web scraping.
- Paid data APIs.
- Optional external AI integrations.

These are intentionally excluded from the local-first MVP.
