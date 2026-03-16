"""
Referee Agent Module (Ollama Powered)

Bounded AI agent for generating explanations in gray-zone cases.
IMPORTANT: Operates in "explain-only" mode - never makes decisions.
"""

import json
import logging
import httpx
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

logger = logging.getLogger(__name__)

@dataclass
class RefereeExplanation:
    """An AI-generated explanation for a match pair."""
    explanation_id: str
    pair_id: str
    run_id: str
    explanation_text: str
    judgement: str  # MATCH, NO_MATCH, UNSURE
    evidence_summary: Dict[str, Any]
    model_name: str
    model_version: str
    created_at: datetime


class RefereeAgent:
    """
    Bounded Referee Agent for generating match explanations using Ollama.
    
    Now capable of providing a Judgement (MATCH / NO_MATCH / UNSURE)
    based on the provided evidence.
    """
    
    def __init__(
        self,
        model_name: str = "llama3.2:3b",
        model_version: str = "v2-judgement",
        score_range: tuple = (0.45, 0.85), # Slightly wider range for gray zone
        ollama_url: str = "http://10.11.200.109:11434/api/generate"
    ):
        self.model_name = model_name
        self.model_version = model_version
        self.score_range = score_range
        self.ollama_url = ollama_url
        self._explanations: Dict[str, RefereeExplanation] = {}
        self._use_fallback = False
        self._cache_file = "data/referee_cache.jsonl"
        self._load_cache()

    def _load_cache(self):
        """Load explanations from disk cache."""
        try:
            with open(self._cache_file, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        # Reconstruct object (basic)
                        expl = RefereeExplanation(
                            explanation_id=data['explanation_id'],
                            pair_id=data['pair_id'],
                            run_id=data['run_id'],
                            explanation_text=data['explanation_text'],
                            judgement=data.get('judgement', 'UNSURE'),
                            evidence_summary=data['evidence_summary'],
                            model_name=data['model_name'],
                            model_version=data['model_version'],
                            created_at=datetime.fromisoformat(data['created_at'])
                        )
                        self._explanations[data['pair_id']] = expl
                    except Exception as e:
                        logger.warning(f"Failed to load cache line: {e}")
            logger.info(f"Loaded {len(self._explanations)} explanations from cache")
        except FileNotFoundError:
            logger.info("No cache file found, starting fresh")

    def _save_to_cache(self, explanation: RefereeExplanation):
        """Append explanation to disk cache."""
        try:
            with open(self._cache_file, 'a') as f:
                data = {
                    'explanation_id': explanation.explanation_id,
                    'pair_id': explanation.pair_id,
                    'run_id': explanation.run_id,
                    'explanation_text': explanation.explanation_text,
                    'judgement': explanation.judgement,
                    'evidence_summary': explanation.evidence_summary,
                    'model_name': explanation.model_name,
                    'model_version': explanation.model_version,
                    'created_at': explanation.created_at.isoformat()
                }
                f.write(json.dumps(data) + '\n')
        except Exception as e:
            logger.error(f"Failed to save to cache: {e}")

    def should_invoke(
        self,
        score: float,
        hard_conflicts: List[str]
    ) -> bool:
        """
        Check if referee *needs* to be invoked (is it in the gray zone?).
        """
        if hard_conflicts:
            return False # Clear reject
        
        min_score, max_score = self.score_range
        return min_score <= score <= max_score
    
    def _call_ollama(self, prompt: str) -> Optional[str]:
        """Call Ollama API."""
        if self._use_fallback:
            return None
            
        try:
            response = httpx.post(
                self.ollama_url,
                json={
                    "model": self.model_name,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 256
                    }
                },
                timeout=30
            )
            if response.status_code == 200:
                text = response.json().get('response', '')
                return text.strip()
            else:
                logger.warning(f"Ollama returned status {response.status_code}")
        except Exception as e:
            logger.warning(f"Ollama call failed ({e}). Switching to fallback template.")
        return None

    def generate_explanation(
        self,
        pair_id: str,
        run_id: str,
        record_a: dict,
        record_b: dict,
        score: float,
        evidence: List[dict],
        signals: List[str],
        hard_conflicts: List[str]
    ) -> RefereeExplanation:
        """
        Generate an explanation for a match pair with judgement.
        Uses cache first, then automated rules, then Ollama.
        """
        # 1. Check Cache (Memory)
        if pair_id in self._explanations:
            return self._explanations[pair_id]

        # 2. Automated Optimization (Skip LLM for obvious cases)
        # If score is very high or very low, provide deterministic judgement
        if not self.should_invoke(score, hard_conflicts):
            # Check why we shouldn't invoke
            if hard_conflicts:
                judgement = "NO_MATCH"
                explanation_text = f"Automated Rejection: Critical conflicts detected ({', '.join(hard_conflicts)})."
            elif score > self.score_range[1]:
                judgement = "MATCH"
                explanation_text = f"Automated Confirmation: Match score ({score:.2f}) is very high and exceeds the verification threshold."
            else:
                judgement = "NO_MATCH"
                explanation_text = f"Automated Rejection: Match score ({score:.2f}) is too low to be considered a potential match."
            
            explanation = RefereeExplanation(
                explanation_id=str(uuid4()),
                pair_id=pair_id,
                run_id=run_id,
                explanation_text=explanation_text,
                judgement=judgement,
                evidence_summary={'score': score, 'automated': True},
                model_name="automated_rule_engine",
                model_version="1.0",
                created_at=datetime.utcnow()
            )
            self._save_to_cache(explanation)
            self._explanations[pair_id] = explanation
            return explanation

        # 3. Call Ollama (Only for Gray Zone)
        # Build prompt
        evidence_text = "\n".join([
            f"- {e['field']}: {e['type']} ({e.get('similarity', 0):.2f})" 
            for e in evidence
        ])
        
        conflicts_text = ""
        if hard_conflicts:
            conflicts_text = f"CRITICAL CONFLICTS: {', '.join(hard_conflicts)}"

        prompt = f"""
You are an expert Data Steward for a Bank. Your task is to judge if two customer records represent the SAME PERSON.

Record A: {json.dumps(record_a, default=str)}
Record B: {json.dumps(record_b, default=str)}

Match Score: {score:.2f}
Evidence Matrix:
{evidence_text}
{conflicts_text}

INSTRUCTIONS:
1. Analyze the fields (Name, Address, DOB, IDs).
2. Look for typos, phonetic matches, or contradictions.
3. A "MATCH" requires strong evidence (same person).
4. If there are contradictions (different gender, different clear IDs), it is "NO_MATCH".
5. If unsure, say "UNSURE".

OUTPUT FORMAT:
JUDGEMENT: [MATCH | NO_MATCH | UNSURE]
ANALYSIS: [Short, clear explanation in 1-2 sentences]
"""

        raw_response = self._call_ollama(prompt)
        
        judgement = "UNSURE"
        explanation_text = "AI service unavailable. Using template fallback."

        if raw_response:
            # Parse response
            lines = raw_response.split('\n')
            accumulated_analysis = []
            
            for line in lines:
                clean_line = line.strip()
                if clean_line.upper().startswith("JUDGEMENT:"):
                    parts = clean_line.split(":", 1)
                    if len(parts) > 1:
                        j_val = parts[1].strip().upper()
                        if "NO_MATCH" in j_val or "NO MATCH" in j_val:
                            judgement = "NO_MATCH"
                        elif "MATCH" in j_val and "NO" not in j_val:
                            judgement = "MATCH"
                        else:
                            judgement = "UNSURE"
                elif clean_line.upper().startswith("ANALYSIS:"):
                    accumulated_analysis.append(clean_line.split(":", 1)[1].strip())
                elif clean_line and not clean_line.upper().startswith("JUDGEMENT"):
                    accumulated_analysis.append(clean_line)
            
            if accumulated_analysis:
                explanation_text = " ".join(accumulated_analysis)
            else:
                explanation_text = raw_response # Fallback to raw if parsing fails
        else:
             explanation_text = self._generate_template_explanation(score, evidence)

        # Create explanation record
        explanation = RefereeExplanation(
            explanation_id=str(uuid4()),
            pair_id=pair_id,
            run_id=run_id,
            explanation_text=explanation_text,
            judgement=judgement,
            evidence_summary={
                'score': score,
                'signals_hit': signals,
                'hard_conflicts': hard_conflicts,
                'field_comparisons': evidence
            },
            model_name=self.model_name,
            model_version=self.model_version,
            created_at=datetime.utcnow()
        )
        
        self._explanations[pair_id] = explanation
        self._save_to_cache(explanation)
        return explanation

    def _generate_template_explanation(self, score: float, evidence: List[dict]) -> str:
        """Fallback template-based explanation."""
        parts = []
        parts.append("## Match Analysis (Template Fallback)")
        parts.append(f"**Match Score:** {score:.1%}")
        parts.append("\n### Evidence:")
        for ev in evidence:
            parts.append(f"- **{ev['field']}**: {ev['type']} ({ev.get('similarity', 0):.0%})")
        parts.append("\n### Guidance:")
        parts.append("Please review the fields manually.")
        return "\n".join(parts)

    def get_explanation(self, pair_id: str) -> Optional[RefereeExplanation]:
        return self._explanations.get(pair_id)

    def has_explanation(self, pair_id: str) -> bool:
        return pair_id in self._explanations

# Singleton
_referee = RefereeAgent()

def get_referee() -> RefereeAgent:
    return _referee

