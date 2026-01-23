# Frontend Source Reference

> **Last Updated**: 2026-01-19
> **Purpose**: Next.js 14 frontend for AI Agent Platform

## Tech Stack
- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **State**: React Context

---

## Quick Navigation

| Folder | Purpose | Key Files |
|--------|---------|-----------|
| [app/](#app) | Next.js pages (App Router) | `page.tsx`, `layout.tsx` |
| [components/](#components) | React components | UI, incidents, workflow |
| [contexts/](#contexts) | React Context providers | Theme, auth |
| [lib/](#lib) | Utilities and API clients | `api.ts` |
| [types/](#types) | TypeScript type definitions | `index.ts`, `agent.ts` |

---

## Folder Details

### app/
Next.js 14 App Router pages.

| Path | Purpose |
|------|---------|
| `page.tsx` | **Home** - Dashboard with stats, incidents |
| `layout.tsx` | Root layout with providers |
| `globals.css` | Global styles |
| `incidents/` | Incident management pages |
| `incidents/[id]/page.tsx` | Incident detail view |
| `jira/` | Jira story pages |
| `jira/[id]/page.tsx` | Jira story detail |
| `graph/` | LangGraph visualization |
| `graph/[id]/page.tsx` | Workflow graph view |
| `agents/` | Agent management |
| `approvals/` | HITL approval pages |
| `events/` | Event stream |
| `settings/` | Application settings |
| `workflows/` | Unified workflow view |

---

### components/
Reusable React components organized by domain.

#### components/ui/
Base UI components (atomic design).

| File | Purpose |
|------|---------|
| `Button.tsx` | Button component |
| `Card.tsx` | Card container |
| `Modal.tsx` | Modal dialog |
| `Input.tsx` | Form input |
| `Select.tsx` | Dropdown select |
| `Table.tsx` | Data table |
| `Tabs.tsx` | Tab navigation |
| `Badge.tsx` | Status badge |
| `StatusBadge.tsx` | Colored status indicators |
| `LoadingSpinner.tsx` | Loading indicator |

#### components/layout/
Layout components.

| File | Purpose |
|------|---------|
| `Header.tsx` | Top navigation bar |
| `Sidebar.tsx` | Left navigation sidebar |
| `PageLayout.tsx` | Page wrapper layout |

#### components/dashboard/
Dashboard widgets.

| File | Purpose |
|------|---------|
| `StatsCard.tsx` | Statistics card |
| `AgentStatus.tsx` | Agent health status |
| `ActivityChart.tsx` | Activity graph |
| `RecentIncidents.tsx` | Recent incidents list |
| `SystemHealth.tsx` | System health overview |

#### components/incidents/
Incident management components.

| File | Purpose |
|------|---------|
| `IncidentTable.tsx` | Incident list table |
| `IncidentDetail.tsx` | Incident details view |
| `EnterpriseIncidentDetail.tsx` | Enhanced incident detail |
| `IncidentFilters.tsx` | Filter controls |
| `IncidentWorkflow.tsx` | Workflow visualization |
| `IncidentChat.tsx` | Chat interface for incident |
| `RemediationPanel.tsx` | Remediation controls |
| `RAGPanel.tsx` | RAG search results |
| `CreateIncidentModal.tsx` | New incident form |

#### components/workflow/
LangGraph workflow visualization.

| File | Purpose |
|------|---------|
| `WorkflowVisualization.tsx` | Graph node visualization |

#### components/pipeline/
Data pipeline components.

| File | Purpose |
|------|---------|
| `PipelineConfigForm.tsx` | Pipeline configuration form |
| `PipelineProgress.tsx` | Pipeline generation progress |

#### components/chat/
Chat interface components.

| File | Purpose |
|------|---------|
| `ChatWrapper.tsx` | Chat container |
| `FloatingChat.tsx` | Floating chat button |

#### components/agents/
Agent management components.

| File | Purpose |
|------|---------|
| `AgentGrid.tsx` | Agent card grid |
| `AgentMetrics.tsx` | Agent metrics display |
| `AgentLogs.tsx` | Agent log viewer |

#### components/events/
Event stream components.

| File | Purpose |
|------|---------|
| `EventStream.tsx` | Real-time event list |
| `EventCard.tsx` | Individual event card |
| `EventFilter.tsx` | Event type filter |

#### components/approvals/
HITL approval components.

| File | Purpose |
|------|---------|
| `ApprovalList.tsx` | Pending approvals list |

---

### contexts/
React Context providers for global state.

| File | Purpose |
|------|---------|
| (provider files) | Theme, auth, notification contexts |

---

### lib/
Utilities, API clients, and helpers.

| File | Purpose |
|------|---------|
| `api.ts` | Backend API client |
| (other utils) | Helper functions |

---

### types/
TypeScript type definitions.

| File | Purpose |
|------|---------|
| `index.ts` | Core types (Incident, Script, etc.) |
| `agent.ts` | Agent-related types |
| `pipeline.ts` | Pipeline types |

---

## API Integration

The frontend connects to the backend API at `NEXT_PUBLIC_API_URL`.

```typescript
// Example API usage
const response = await fetch(`${API_URL}/api/incidents`);
const { incidents } = await response.json();
```

**Key Endpoints Used**:
- `GET /api/incidents` - List incidents
- `GET /api/incidents/:id` - Get incident
- `POST /api/scripts/match` - Match scripts to incident
- `POST /api/execute` - Execute remediation
- `GET /api/approvals` - Get pending approvals
- `POST /api/approvals/:id/approve` - Approve execution
- `GET /api/langgraph/definition` - Get workflow definition
- `GET /api/jira/stories` - Get Jira stories
- `GET /api/pipelines` - Get pipelines

---

## Development

```bash
# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build

# Start production server
npm start
```

---

## Environment Variables

```env
# .env.local
NEXT_PUBLIC_API_URL=http://localhost:8000
```
