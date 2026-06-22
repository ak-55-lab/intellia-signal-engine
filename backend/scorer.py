"""
Claude scoring pipeline — two-tier:
  Tier 2: Haiku triage — score all items, skip if < threshold
  Tier 3: Sonnet verification — verify items >= verify_threshold

KB context from kb.py is injected into the system prompt.
Each scored result carries a `_trace` key with full observability data.
"""

import os
import json
import asyncio
import anthropic
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple
import config

_client = None
_client_key: str = ""   # rebuild client if key changes at runtime

def get_client():
    global _client, _client_key
    current_key = config.get("ANTHROPIC_API_KEY")
    if _client is None or current_key != _client_key:
        _client = anthropic.Anthropic(api_key=current_key)
        _client_key = current_key
    return _client

HAIKU_MODEL  = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"


def build_system_prompt(kb_context: str = "") -> str:
    base = (
        "You are a B2B sales signal analyst specializing in EHS, quality management, "
        "and compliance software for the healthcare sector.\n\n"
    )
    if kb_context:
        base += f"Knowledge base context — use this to inform your scoring:\n\n{kb_context}\n\n"
    base += (
        "Your job: given raw data about an account, score it 1-10 as a buying signal "
        "for EHS/quality/compliance software. Always respond with valid JSON only — "
        "no markdown, no explanation outside the JSON object."
    )
    return base


VERIFY_TEMPLATE = """
Re-evaluate this signal for {account_name}. A first-pass triage scored it {initial_score}/10.

Raw data:
{data}

Original prompt context: {prompt}

Verify: Is this org the regulated party (not the regulator)? Is the event serious enough to create urgency?
Re-score carefully.

Respond in JSON only:
{{"score":int,"summary":"one sentence on what happened and why it matters","action":"one sentence recommended next step","excerpt":"most compelling detail quoted or paraphrased","verified":true,"confidence":"high|medium|low"}}
"""


async def _call_claude(
    model: str,
    system: str,
    user_message: str,
    max_tokens: int = 512,
) -> Tuple[Optional[dict], str, int, int]:
    """Returns (parsed_result, raw_text, input_tokens, output_tokens)."""
    try:
        resp = get_client().messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = resp.content[0].text.strip()
        in_tok  = resp.usage.input_tokens  if resp.usage else 0
        out_tok = resp.usage.output_tokens if resp.usage else 0
        # Strip markdown fences if present
        clean = raw
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        return json.loads(clean.strip()), raw, in_tok, out_tok
    except Exception as e:
        print(f"[scorer] Claude call failed ({model}): {e}")
        return None, str(e), 0, 0


async def score_signal(
    account_name: str,
    data_text: str,
    column_key: str,
    prompt_template: str,
    triage_threshold: int = 4,
    verify_threshold: int = 6,
    kb_context: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Score one data block for one account+column.
    Returns signal dict (with `_trace` key) or None.
    """
    if not data_text or "No " in data_text[:20]:
        return None

    system = build_system_prompt(kb_context)
    normalized   = prompt_template.replace("{account}", "{account_name}")
    user_prompt  = normalized.format(account_name=account_name)
    user_message = f"{user_prompt}\n\nData:\n{data_text}"

    trace = {
        "system_prompt":   system,
        "triage": {
            "model":         HAIKU_MODEL,
            "prompt":        user_prompt,
            "data_preview":  data_text[:800],
            "data_chars":    len(data_text),
            "raw_response":  None,
            "parsed":        None,
            "input_tokens":  0,
            "output_tokens": 0,
        },
        "verify": None,
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }

    # Tier 2: Haiku triage
    result, raw_text, in_tok, out_tok = await _call_claude(
        HAIKU_MODEL, system, user_message, max_tokens=512
    )
    trace["triage"].update({
        "raw_response":  raw_text,
        "parsed":        result,
        "input_tokens":  in_tok,
        "output_tokens": out_tok,
    })

    if not result:
        return None

    result["source_type"] = column_key
    result["model"]       = HAIKU_MODEL

    score = result.get("score", 0)
    if score < triage_threshold:
        result["_trace"] = trace
        return result

    # Tier 3: Sonnet verification
    if score >= verify_threshold:
        verify_prompt = VERIFY_TEMPLATE.format(
            account_name=account_name,
            initial_score=score,
            data=data_text,
            prompt=prompt_template[:200],
        )
        verified, v_raw, v_in, v_out = await _call_claude(
            SONNET_MODEL, system, verify_prompt, max_tokens=768
        )
        trace["verify"] = {
            "model":         SONNET_MODEL,
            "prompt":        verify_prompt,
            "raw_response":  v_raw,
            "parsed":        verified,
            "input_tokens":  v_in,
            "output_tokens": v_out,
        }
        if verified:
            verified["source_type"] = column_key
            verified["model"]       = SONNET_MODEL
            verified["_trace"]      = trace
            return verified

    result["_trace"] = trace
    return result


async def score_all_columns(
    account_name: str,
    column_data: Dict[str, str],
    column_configs: Dict[str, Any],
    triage_threshold: int = 4,
    verify_threshold: int = 6,
    kb_context: str = "",
) -> Dict[str, Optional[Dict[str, Any]]]:
    results = {}
    for col_key, data_text in column_data.items():
        col = column_configs.get(col_key, {})
        if not col.get("on", True):
            continue
        prompt = col.get("prompt", "Score this data 1-10. Respond in JSON.")
        result = await score_signal(
            account_name=account_name,
            data_text=data_text,
            column_key=col_key,
            prompt_template=prompt,
            triage_threshold=triage_threshold,
            verify_threshold=verify_threshold,
            kb_context=kb_context,
        )
        results[col_key] = result
        await asyncio.sleep(0.3)
    return results
