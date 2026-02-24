"""
CUIN v2 - Splink Scoring Module

Probabilistic record matching using Splink logic (custom implementation for control).
"""

import json
from typing import List, Dict, Optional, Tuple, Any
import logging

from engine.structures import MatchScore, FieldEvidence, ScoringConfig

# Optional imports
try:
    import jellyfish
    HAS_JELLYFISH = True
except ImportError:
    HAS_JELLYFISH = False

logger = logging.getLogger(__name__)


class SplinkScorer:
    """
    Splink-based probabilistic record matcher.
    
    Uses a combination of exact and fuzzy matching with
    explainable evidence for each comparison.
    """
    
    def __init__(self, config: Optional[ScoringConfig] = None):
        self.config = config or ScoringConfig()
    
    def _jaro_winkler(self, s1: str, s2: str) -> float:
        """Calculate Jaro-Winkler similarity."""
        if not HAS_JELLYFISH:
            return 1.0 if s1 == s2 else 0.0
        
        if not s1 or not s2:
            return 0.0
        
        return jellyfish.jaro_winkler_similarity(s1, s2)
    
    def _compare_exact(
        self,
        field_name: str,
        val_a: Optional[str],
        val_b: Optional[str],
        weight: float
    ) -> FieldEvidence:
        """Compare two values for exact match."""
        if not val_a and not val_b:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="both_null",
                similarity_score=0.0,
                match_weight=0.0,
                explanation="Both values are null - no evidence"
            )
        
        if not val_a or not val_b:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="one_null",
                similarity_score=0.0,
                match_weight=0.0,
                explanation=f"One value is null ({field_name})"
            )
        
        if val_a == val_b:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="exact_match",
                similarity_score=1.0,
                match_weight=weight,
                explanation=f"Exact match on {field_name}"
            )
        
        return FieldEvidence(
            field_name=field_name,
            value_a=val_a,
            value_b=val_b,
            comparison_type="mismatch",
            similarity_score=0.0,
            match_weight=-weight * 0.5,  # Slight penalty for mismatch
            explanation=f"Values differ for {field_name}"
        )
    
    def _compare_fuzzy(
        self,
        field_name: str,
        val_a: Optional[str],
        val_b: Optional[str],
        weight: float,
        threshold: float = 0.85
    ) -> FieldEvidence:
        """Compare two values with fuzzy matching."""
        if not val_a and not val_b:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="both_null",
                similarity_score=0.0,
                match_weight=0.0,
                explanation="Both values are null - no evidence"
            )
        
        if not val_a or not val_b:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="one_null",
                similarity_score=0.0,
                match_weight=0.0,
                explanation=f"One value is null ({field_name})"
            )
        
        similarity = self._jaro_winkler(val_a, val_b)
        
        if similarity >= 0.99:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="exact_match",
                similarity_score=similarity,
                match_weight=weight,
                explanation=f"Exact match on {field_name}"
            )
        
        if similarity >= threshold:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="fuzzy_match",
                similarity_score=similarity,
                match_weight=weight * similarity,
                explanation=f"High similarity ({similarity:.1%}) on {field_name}"
            )
        
        if similarity >= 0.7:
            return FieldEvidence(
                field_name=field_name,
                value_a=val_a,
                value_b=val_b,
                comparison_type="low_similarity",
                similarity_score=similarity,
                match_weight=weight * similarity * 0.5,
                explanation=f"Moderate similarity ({similarity:.1%}) on {field_name}"
            )
        
        return FieldEvidence(
            field_name=field_name,
            value_a=val_a,
            value_b=val_b,
            comparison_type="mismatch",
            similarity_score=similarity,
            match_weight=-weight * 0.3,
            explanation=f"Low similarity ({similarity:.1%}) on {field_name}"
        )
    
    def _check_hard_conflicts(
        self,
        record_a: dict,
        record_b: dict
    ) -> List[str]:
        """
        Check for hard conflicts that should prevent auto-linking.
        """
        conflicts = []
        
        # Check for Strong Identity Signals to override Hard Conflicts
        # We define a "Strong Identity" as:
        # 1. Exact Name + Exact Email (Previous fix)
        # 2. High Fuzzy Name (>0.85) + High Fuzzy Address (>0.85) (New case from screenshot)
        # 3. High Fuzzy Name (>0.85) + Exact Phone
        # 4. High Fuzzy Name (>0.85) + Exact Email
        
        name_weight = 0.0
        other_strong_signal = False
        
        # Check Name
        name_a = record_a.get('name_norm')
        name_b = record_b.get('name_norm')
        if name_a and name_b:
            # Re-calculate similarity (duplicate logic, but needed for conflict check context)
            sim = self._jaro_winkler(name_a, name_b)
            if sim == 1.0:
                name_weight = 1.0
            elif sim >= 0.85:
                name_weight = 0.85
        
        # Check Other Signals
        email_a = record_a.get('email_norm')
        email_b = record_b.get('email_norm')
        if email_a and email_b and email_a == email_b:
            other_strong_signal = True
            
        phone_a = record_a.get('phone_norm')
        phone_b = record_b.get('phone_norm')
        if phone_a and phone_b and phone_a == phone_b:
            other_strong_signal = True
            
        addr_a = record_a.get('address_norm')
        addr_b = record_b.get('address_norm')
        if addr_a and addr_b:
            sim = self._jaro_winkler(addr_a, addr_b)
            if sim >= 0.85: # High fuzzy address
                other_strong_signal = True

        has_strong_identity = (name_weight >= 0.85 and other_strong_signal)
        
        # NatID conflict: both present but different
        natid_a = record_a.get('natid_norm')
        natid_b = record_b.get('natid_norm')
        if natid_a and natid_b and natid_a != natid_b:
            if has_strong_identity:
                # Log or just ignore as hard conflict?
                # For now, let's treat it as a signal but NOT a hard blocker
                pass 
            else:
                conflicts.append("natid_mismatch")
        
        # DOB conflict: both present but differ by > 2 years
        dob_a = record_a.get('dob_norm')
        dob_b = record_b.get('dob_norm')
        if dob_a and dob_b:
            try:
                from datetime import datetime
                date_a = datetime.strptime(dob_a, '%Y-%m-%d')
                date_b = datetime.strptime(dob_b, '%Y-%m-%d')
                year_diff = abs(date_a.year - date_b.year)
                if year_diff > 2:
                    conflicts.append("dob_major_mismatch")
            except Exception:
                pass
        
        return conflicts
    
    def _check_signals(
        self,
        evidence: List[FieldEvidence]
    ) -> List[str]:
        """
        Check which positive signals are hit.
        """
        signals = []
        
        for ev in evidence:
            if ev.comparison_type == "exact_match":
                if ev.field_name == "phone_norm":
                    signals.append("exact_phone")
                elif ev.field_name == "email_norm":
                    signals.append("exact_email")
                elif ev.field_name == "natid_norm":
                    signals.append("exact_natid")
                elif ev.field_name == "dob_norm":
                    signals.append("exact_dob")
                elif ev.field_name == "name_norm":
                    signals.append("exact_name")
                elif ev.field_name == "address_norm":
                    signals.append("exact_address")
            
            # Fuzzy signals (only if not exact, or generally high)
            # We add fuzzy signal ONLY if it's NOT exact, to distinguish them.
            if ev.comparison_type != "exact_match":
                if ev.field_name == "name_norm" and ev.similarity_score >= 0.85:
                    signals.append("fuzzy_name_high")
                
                if ev.field_name == "address_norm" and ev.similarity_score >= 0.80:
                    signals.append("address_high")
        
        return signals
    
    def score_pair(
        self,
        pair_id: str,
        record_a: dict,
        record_b: dict
    ) -> MatchScore:
        """
        Score a single candidate pair.
        """
        # FAST PATH: Exact Record Hash Match
        # If the raw data is identical, it's a 100% match
        if record_a.get('record_hash') and record_a.get('record_hash') == record_b.get('record_hash'):
             return MatchScore(
                pair_id=pair_id,
                a_key=record_a.get('customer_key') or record_a.get('source_customer_id', ''),
                b_key=record_b.get('customer_key') or record_b.get('source_customer_id', ''),
                score=1.0,
                evidence=[
                    self._compare_exact("record_hash", record_a.get('record_hash'), record_b.get('record_hash'), 1.0)
                ],
                hard_conflicts=[],
                signals_hit=["exact_hash_match", "auto_link_forced"]
            )

        evidence = []
        
        # Compare name (fuzzy)
        evidence.append(self._compare_fuzzy(
            "name_norm",
            record_a.get('name_norm'),
            record_b.get('name_norm'),
            self.config.name_weight,
            threshold=0.85
        ))
        
        # Compare phone (exact)
        evidence.append(self._compare_exact(
            "phone_norm",
            record_a.get('phone_norm'),
            record_b.get('phone_norm'),
            self.config.phone_weight
        ))
        
        # Compare email (exact)
        evidence.append(self._compare_exact(
            "email_norm",
            record_a.get('email_norm'),
            record_b.get('email_norm'),
            self.config.email_weight
        ))
        
        # Compare DOB (exact)
        evidence.append(self._compare_exact(
            "dob_norm",
            record_a.get('dob_norm'),
            record_b.get('dob_norm'),
            self.config.dob_weight
        ))
        
        # Compare NatID (exact)
        evidence.append(self._compare_exact(
            "natid_norm",
            record_a.get('natid_norm'),
            record_b.get('natid_norm'),
            self.config.natid_weight
        ))
        
        # Compare address (fuzzy)
        evidence.append(self._compare_fuzzy(
            "address_norm",
            record_a.get('address_norm'),
            record_b.get('address_norm'),
            self.config.address_weight,
            threshold=0.80
        ))
        
        # Calculate Score using Adaptive Weighted Average
        # S = (Sum of weights for Matches) / (Sum of weights for Non-Null Comparisons)
        
        matched_weight = 0.0
        available_weight = 0.0
        
        for ev in evidence:
            # We only count a field as "available" if at least one side is not null
            # Or actually, typically we only score if BOTH are present.
            # If one is missing, it's ambiguous. 
            # Current evidence logic:
            # - both_null -> weight 0
            # - one_null -> weight 0
            # - match/mismatch -> weight set
            
            # Use the magnitude of the configured weight for the field to determine importance
            # We need to map back evidence to config weights roughly, or just use the match_weight if positive
            # But mismatch has negative weight.
            
            # Let's count "available_weight" as the weight of this field in the config
            # if comparison_type is NOT 'both_null'.
            # If 'one_null', we penalize slightly? Or ignore?
            # Standard ER: Ignore Nulls (don't penalize, don't reward).
            
            field_weight = 0.0
            if ev.field_name == "name_norm": field_weight = self.config.name_weight
            elif ev.field_name == "phone_norm": field_weight = self.config.phone_weight
            elif ev.field_name == "email_norm": field_weight = self.config.email_weight
            elif ev.field_name == "dob_norm": field_weight = self.config.dob_weight
            elif ev.field_name == "natid_norm": field_weight = self.config.natid_weight
            elif ev.field_name == "address_norm": field_weight = self.config.address_weight
            
            if ev.comparison_type != "both_null" and ev.comparison_type != "one_null":
                # Valid comparison
                available_weight += field_weight
                
                if ev.match_weight > 0:
                    matched_weight += ev.match_weight
                else:
                    # Mismatch: weight is negative in current logic, implies 0 contribution to numerator
                    # AND we keep it in denominator, so it lowers score. 
                    pass
            
            # If one_null, we skipping it (Adaptive). 
            # This means if I have Name matches (0.25) and Phone matches (0.20) and everything else Null
            # Score = (0.45) / (0.45) = 1.0 (Perfect match on available data!)
            
        # Safety: avoid divide by zero
        if available_weight < 0.1:
            score = 0.0
        else:
            score = matched_weight / available_weight
            
        # Penalize slightly if total evidence is low (e.g. only name matched)
        # to prevent "FirstName Only" matches from being 100% confidence
        if available_weight < 0.4 and score > 0.8:
            # EXCEPTION: If we have an EXACT name match, we trust it more
            name_evidence = next((e for e in evidence if e.field_name == "name_norm"), None)
            is_exact_name = name_evidence and name_evidence.comparison_type == "exact_match"
            
            # EXCEPTION 2: High confidence fuzzy match on a LONG name
            # For "GOLAM MOHD ZUBAYED A" vs "GOLAM MOHAMMED ZUBAYED A", similarity is > 0.9
            # and name is long (high entropy), so we trust it.
            is_high_fuzzy = (name_evidence and 
                             name_evidence.comparison_type == "fuzzy_match" and
                             name_evidence.similarity_score > 0.88 and
                             len(str(name_evidence.value_a or '')) > 10)
            
            if not is_exact_name and not is_high_fuzzy:
                score = 0.8 # Cap confidence for low-info matches only if not high-fuzzy
        
        # USER RULE: High Similarity Name Match -> Boost Score
        # If name similarity is very high (>0.9) and name is long, trust it significantly
        name_ev = next((e for e in evidence if e.field_name == "name_norm"), None)
        if name_ev and name_ev.similarity_score >= 0.90:
            # Boost score to at least 0.93 (above auto-link 0.92) if name is long enough
            if len(str(name_ev.value_a or '')) > 8:
                 if score < 0.93:
                     score = 0.93
        elif name_ev and name_ev.similarity_score >= 0.85:
            # Boost moderate-high fuzzy matches (e.g. MOHD vs MOHAMMED) to 0.90
            # Needs one more signal or slight boost to cross 0.92 if very close
             if len(str(name_ev.value_a or '')) > 10:
                 if score < 0.90:
                     score = 0.90
        
        # USER RULE: Exact Name Match + Any Other Field Match => Auto-Link
        # Fixes issue where mismatches in other fields drag score down despite strong identity
        name_ev = next((e for e in evidence if e.field_name == "name_norm"), None)
        if name_ev and name_ev.comparison_type == "exact_match":
            # Check if any other field contributes positively (fuzzy or exact)
            other_match_found = any(
                e.match_weight > 0 
                for e in evidence 
                if e.field_name != "name_norm"
            )
            
            if other_match_found:
                # Boost score to ensure auto-link (unless hard conflict exists)
                if score < 0.98:
                    score = 0.98

        # Check for hard conflicts
        hard_conflicts = self._check_hard_conflicts(record_a, record_b)
        if hard_conflicts:
            score = 0.1
        
        # Check signals
        signals = self._check_signals(evidence)
        
        return MatchScore(
            pair_id=pair_id,
            a_key=record_a.get('customer_key') or record_a.get('source_customer_id', ''),
            b_key=record_b.get('customer_key') or record_b.get('source_customer_id', ''),
            score=min(1.0, max(0.0, score)),
            evidence=evidence,
            hard_conflicts=hard_conflicts,
            signals_hit=signals
        )
    
    def evidence_to_json(self, evidence: List[FieldEvidence]) -> str:
        """Convert evidence list to JSON string for storage."""
        return json.dumps([
            {
                'field': ev.field_name,
                'value_a': ev.value_a,
                'value_b': ev.value_b,
                'type': ev.comparison_type,
                'similarity': ev.similarity_score,
                'weight': ev.match_weight,
                'explanation': ev.explanation,
            }
            for ev in evidence
        ])
