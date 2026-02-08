"""
CUIN v2 Control Plane - Configuration Routes

Endpoints for managing dynamic pipeline settings.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from engine.blocking.multipass_blocker import BlockingConfig
from engine.structures import ScoringConfig

router = APIRouter()

# Global in-memory configuration state
# In production, this might be stored in Redis/DB
class DynamicConfig:
    blocking = BlockingConfig()
    scoring = ScoringConfig()

_config_store = DynamicConfig()

def get_current_blocking_config() -> BlockingConfig:
    return _config_store.blocking

def get_current_scoring_config() -> ScoringConfig:
    return _config_store.scoring

# ----------------------------------------
# Request / Response Models
# ----------------------------------------

class UpdateConfigRequest(BaseModel):
    # Blocking
    blocking_max_block_size: Optional[int] = None
    blocking_suppress_pct: Optional[float] = None
    blocking_lsh_threshold: Optional[float] = None
    
    # Matching Weights
    match_name_weight: Optional[float] = None
    match_phone_weight: Optional[float] = None
    match_email_weight: Optional[float] = None
    match_dob_weight: Optional[float] = None
    match_natid_weight: Optional[float] = None
    match_address_weight: Optional[float] = None
    
    # Thresholds (Used by Planner/Refereee) which we might want to expose here too
    # but currently ScoringConfig handles weights, not decision thresholds. 
    # For simplicity, we just stick to weight/blocking tuning.

class ConfigResponse(BaseModel):
    blocking: dict
    scoring: dict


# ----------------------------------------
# Routes
# ----------------------------------------

@router.get("", response_model=ConfigResponse)
async def get_config() -> dict:
    """Get current pipeline configuration."""
    return {
        "blocking": _config_store.blocking.__dict__,
        "scoring": _config_store.scoring.__dict__
    }

@router.post("", response_model=ConfigResponse)
async def update_config(request: UpdateConfigRequest) -> dict:
    """Update pipeline configuration."""
    
    # Update Blocking
    if request.blocking_max_block_size is not None:
        _config_store.blocking.max_block_size = request.blocking_max_block_size
    if request.blocking_suppress_pct is not None:
        _config_store.blocking.suppress_frequency_pct = request.blocking_suppress_pct
    if request.blocking_lsh_threshold is not None:
        _config_store.blocking.lsh_threshold = request.blocking_lsh_threshold
        
    # Update Scoring
    if request.match_name_weight is not None:
        _config_store.scoring.name_weight = request.match_name_weight
    if request.match_phone_weight is not None:
        _config_store.scoring.phone_weight = request.match_phone_weight
    if request.match_email_weight is not None:
        _config_store.scoring.email_weight = request.match_email_weight
    if request.match_dob_weight is not None:
        _config_store.scoring.dob_weight = request.match_dob_weight
    if request.match_natid_weight is not None:
        _config_store.scoring.natid_weight = request.match_natid_weight
    if request.match_address_weight is not None:
        _config_store.scoring.address_weight = request.match_address_weight

    return {
        "blocking": _config_store.blocking.__dict__,
        "scoring": _config_store.scoring.__dict__
    }
