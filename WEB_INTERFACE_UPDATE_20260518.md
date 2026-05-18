# Web Interface Update Notes - 2026-05-18

This document summarizes the May 18, 2026 update to the MsAb-PatKG Intelligence Streamlit interface.

## Scope

The update focuses on making the deployed web application easier for manuscript readers and external users to understand and use without reading the paper first.

Main goals:

- Present a professional blue-white interface with clear top navigation.
- Define the platform scope before users ask questions.
- Prevent unresolved anti-infective placeholder categories from being ranked as resolved human target pairs.
- Add a user feedback workflow for unsatisfactory answers.
- Improve first-page loading time.

## User Interface Changes

The Streamlit app now uses a four-section layout:

- `Introduction`
- `Scenario`
- `Ask`
- `Contact`

The top navigation bar uses a blue background with white brand text and hover states. The landing page includes a hero section, database coverage cards, supported use cases, data-scope notes, and limitations.

The `Ask` page now includes clear scope cards:

- `Supported Questions`
- `Not Supported`

These cards explain that MsAb-PatKG is a patent-landscape analysis tool, not a biological target-pair prediction engine.

## Database Coverage Display

The Introduction page displays snapshot statistics verified on May 11, 2026:

- Patent families: 1,421
- Patent publications: 16,539
- Resolved target pairs: 700
- Targets: 383
- Pathways: 1,028
- Functional categories: 55
- Assignees: 727
- Technology classes: 14

The Introduction page uses these verified snapshot values so it can load quickly. Live graph-backed querying starts when the user enters `Scenario` or `Ask`.

## Resolved Target-Pair Handling

Target-pair ranking, diversity, count, and functional-combination analyses now exclude unresolved placeholder categories by requiring:

```cypher
tp.name CONTAINS '/'
```

This prevents broad placeholder categories such as anti-infective external-antigen records from appearing as resolved human target-pair identities.

The app also shows a caveat when an answer uses resolved target-pair scope.

## Feedback Workflow

After an answer is generated, users can submit structured feedback from the answer panel.

The feedback payload includes:

- Feedback ID
- Issue type
- User comment
- Original question
- Rewritten question
- Selected intent
- Answer excerpt
- Cypher and returned-row summary

Feedback is saved locally as JSONL under:

```text
feedback_submissions/msab_patkg_feedback.jsonl
```

The generated mailto link sends feedback to:

```text
WANG MENGYANG <cpuwangmengyang@163.com>
```

The contact name and email can be overridden through environment variables:

```text
MSAB_CONTACT_NAME
MSAB_CONTACT_EMAIL
```

## Performance Changes

The Introduction page no longer initializes Neo4j, LLM, entity lookup, or the full query service.

Caching was added:

- `build_services()` uses `st.cache_resource`
- database statistics use `st.cache_data(ttl=3600)`
- entity guide examples and counts use `st.cache_data(ttl=3600)`

This reduces first-page load time and avoids repeated Neo4j queries during normal navigation.

## Verification

The following checks were run locally:

```bash
E:\anconda\envs\bsab-env\python.exe -m py_compile bsab_kg_qa_en\app\app_interactive.py
```

Streamlit AppTest checks were run for:

- Introduction page
- Contact page
- Ask scope gate
- Intent confirmation state

Browser verification confirmed:

- Ask scope cards render at equal height.
- Contact displays `WANG MENGYANG` and `cpuwangmengyang@163.com`.
- The previous `NameError: name 'flow_service' is not defined` is fixed.

## How To Update The Deployed Site

If the Render service is connected to the GitHub repository, the update path is:

1. Commit the changed files.
2. Push to the GitHub branch used by Render, usually `main`.
3. Open the Render dashboard.
4. Confirm that a new deploy starts automatically.
5. After deployment finishes, open the production URL and check:
   - `Introduction`
   - `Scenario`
   - `Ask`
   - `Contact`

If Render does not auto-deploy:

1. Open the Render web service.
2. Click `Manual Deploy`.
3. Choose `Deploy latest commit`.

## Render Environment Variables

Required variables remain:

```text
OPENAI_API_KEY
NEO4J_URI
NEO4J_USER
NEO4J_PASSWORD
NEO4J_DATABASE
```

Optional variables:

```text
OPENAI_MODEL
MSAB_CONTACT_NAME
MSAB_CONTACT_EMAIL
MSAB_FEEDBACK_DIR
```

Recommended contact overrides:

```text
MSAB_CONTACT_NAME=WANG MENGYANG
MSAB_CONTACT_EMAIL=cpuwangmengyang@163.com
```

