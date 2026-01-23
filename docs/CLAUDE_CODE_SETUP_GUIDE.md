# Claude Code Setup Guide for Enterprise Agentic Platform

## 📋 Quick Setup

### Step 1: Place Context Files in Your Project

Copy these files to your project root:

```bash
# In your AI_AGENT_APP directory
cp CLAUDE_CODE_MASTER_CONTEXT.md docs/
cp e2e_validator.py scripts/
```

### Step 2: Update Your CLAUDE.md

Your `CLAUDE.md` should reference the master context. Replace or merge with:

```markdown
# Enterprise Agentic Platform - Claude Code Instructions

## 📚 Required Reading (In Order)

**BEFORE ANY IMPLEMENTATION, READ:**

1. **`docs/CLAUDE_CODE_MASTER_CONTEXT.md`** - Complete platform context
2. **`docs/CLAUDE_CODE_CONTEXT.md`** - Data agent patterns
3. **`docs/ENTERPRISE_AGENTIC_DATA_PLATFORM_README.md`** - Full specification

## 🎯 Quick Reference

### Project Location
- Incident Management: `backend/`
- Data Agent: `agents/data_agent/`
- Frontend: `frontend/`

### Critical Rules
- ✅ USE: LangGraph StateGraph
- ✅ USE: Explicit Pydantic state
- ✅ USE: Jinja2 templates
- ❌ NEVER: ReAct pattern
- ❌ NEVER: Parse free-text
- ❌ NEVER: Auto-deploy to PROD

### Running Tests
\`\`\`bash
# Full validation
python scripts/e2e_validator.py --all

# Quick health check
python scripts/e2e_validator.py --health

# Unit tests only
python scripts/e2e_validator.py --unit
\`\`\`

### Starting Services
\`\`\`bash
docker-compose up -d
./scripts/start_system.sh
\`\`\`

For complete context, see `docs/CLAUDE_CODE_MASTER_CONTEXT.md`
```

### Step 3: Recommended Project Structure

Ensure these key files exist:

```
AI_AGENT_APP/
├── CLAUDE.md                              # ← Entry point for Claude Code
├── docs/
│   ├── CLAUDE_CODE_MASTER_CONTEXT.md     # ← Master context (created above)
│   ├── CLAUDE_CODE_CONTEXT.md            # ← Data agent specifics
│   └── ENTERPRISE_AGENTIC_DATA_PLATFORM_README.md
├── scripts/
│   └── e2e_validator.py                  # ← E2E test validator
├── .env                                   # Environment variables
└── ...
```

---

## 🔧 Claude Code Commands

### Initial Context Loading

When starting Claude Code, it automatically reads `CLAUDE.md`. For complex tasks, prompt it to read additional context:

```
Read docs/CLAUDE_CODE_MASTER_CONTEXT.md before proceeding with implementation
```

### Common Task Prompts

**Adding a New Agent:**
```
I need to add a new agent to the data_agent system. 
Read docs/CLAUDE_CODE_MASTER_CONTEXT.md first, then create a new agent following the base_agent.py pattern.
```

**Running E2E Tests:**
```
Run the e2e_validator.py script with --all flag and fix any failures.
```

**Implementing a New LangGraph Node:**
```
Add a new node to the incident workflow. 
Follow the pattern in backend/orchestrator/langgraph_workflow.py.
```

---

## 📊 Validation Workflow

### Before Committing Changes

```bash
# 1. Run health checks
python scripts/e2e_validator.py --health

# 2. Run unit tests
python scripts/e2e_validator.py --unit

# 3. Run integration tests (requires Docker services)
python scripts/e2e_validator.py --integration

# 4. Run code quality checks
python scripts/e2e_validator.py --quality

# 5. Full validation
python scripts/e2e_validator.py --all
```

### Expected Output

```
============================================================
📋 VALIDATION SUMMARY
============================================================
  Timestamp: 2025-01-18T10:30:00
  Duration:  45.23s
  Total:     20
  ✅ Passed:  18
  ❌ Failed:  0
  ⏭️ Skipped: 1
  ⚠️ Warning: 1
============================================================
🎉 ALL VALIDATIONS PASSED!
```

---

## 🗂️ Key Files Reference

### Configuration
| File | Purpose |
|------|---------|
| `.env` | Environment variables |
| `backend/config/settings.py` | Pydantic settings |
| `backend/config/thresholds.py` | Approval thresholds |

### Workflows
| File | Purpose |
|------|---------|
| `backend/orchestrator/langgraph_workflow.py` | 7-node incident workflow |
| `agents/data_agent/src/graphs/main_graph.py` | 5-agent data workflow |

### RAG System
| File | Purpose |
|------|---------|
| `backend/rag/swarm_retriever.py` | Swarm coordinator |
| `backend/rag/agents/` | 4 search agents |
| `backend/rag/feedback_optimizer.py` | Adaptive learning |

### Tests
| Directory | Purpose |
|-----------|---------|
| `tests/unit/` | Fast isolated tests |
| `tests/integration/` | Service integration |
| `tests/e2e/` | End-to-end workflows |
| `agents/data_agent/tests/` | Data agent tests |

---

## 🆘 Troubleshooting

### Claude Code Can't Find Files

Ensure you're in the correct directory:
```bash
cd /path/to/AI_AGENT_APP
claude  # Start Claude Code from project root
```

### Context Too Large

If the master context is too large, split prompts:
```
First, read just the "Data Engineering Agent" section of docs/CLAUDE_CODE_MASTER_CONTEXT.md
```

### Tests Failing

```bash
# Check infrastructure first
python scripts/e2e_validator.py --health

# Common fixes:
docker-compose restart
./scripts/start_system.sh
```

---

## 📝 Best Practices

1. **Always reference context files** when asking Claude Code to implement features
2. **Run validation** after each significant change
3. **Use --json flag** for CI/CD integration: `python scripts/e2e_validator.py --all --json`
4. **Check health first** before debugging complex issues
5. **Keep CLAUDE.md updated** as the project evolves

---

*Generated for Enterprise Agentic Platform v5.0*