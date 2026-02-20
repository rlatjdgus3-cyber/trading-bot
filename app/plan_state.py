"""
plan_state.py — Formal PLAN state machine for position lifecycle.

States:
  PLAN.NONE → PLAN.INTENT_ENTER → PLAN.ORDER_REQUESTED → PLAN.ENTERING → PLAN.OPEN
  PLAN.OPEN → PLAN.INTENT_EXIT  → PLAN.EXITING        → PLAN.CLOSED   → PLAN.NONE

Functions:
  map_db_to_plan(side, stage, order_state) — replaces _map_strategy_state()
  validate_transition(from_state, to_state) — guard invalid transitions
"""

# ── All valid PLAN states ──
PLAN_NONE = 'PLAN.NONE'
PLAN_INTENT_ENTER = 'PLAN.INTENT_ENTER'
PLAN_ORDER_REQUESTED = 'PLAN.ORDER_REQUESTED'
PLAN_ENTERING = 'PLAN.ENTERING'
PLAN_OPEN = 'PLAN.OPEN'
PLAN_INTENT_EXIT = 'PLAN.INTENT_EXIT'
PLAN_EXITING = 'PLAN.EXITING'
PLAN_CLOSED = 'PLAN.CLOSED'

PLAN_STATES = (
    PLAN_NONE,
    PLAN_INTENT_ENTER,
    PLAN_ORDER_REQUESTED,
    PLAN_ENTERING,
    PLAN_OPEN,
    PLAN_INTENT_EXIT,
    PLAN_EXITING,
    PLAN_CLOSED,
)

# ── Valid transitions ──
# Each key maps to a set of valid next states.
TRANSITIONS = {
    PLAN_NONE:             {PLAN_INTENT_ENTER},
    PLAN_INTENT_ENTER:     {PLAN_ORDER_REQUESTED, PLAN_NONE},         # can cancel back to NONE
    PLAN_ORDER_REQUESTED:  {PLAN_ENTERING, PLAN_NONE},                # order sent or canceled
    PLAN_ENTERING:         {PLAN_OPEN, PLAN_NONE},                    # filled or failed
    PLAN_OPEN:             {PLAN_INTENT_EXIT, PLAN_OPEN, PLAN_NONE},  # exit intent, ADD stays OPEN, or force-heal to NONE
    PLAN_INTENT_EXIT:      {PLAN_EXITING, PLAN_OPEN},                 # order sent or canceled back
    PLAN_EXITING:          {PLAN_CLOSED, PLAN_OPEN},                  # filled or failed
    PLAN_CLOSED:           {PLAN_NONE},                               # always resets
}

# ── order_state DB values → PLAN.* mapping ──
# Used when reading position_state from DB and no plan_state column exists yet.
ORDER_STATE_TO_PLAN_ENTRY = {
    'NONE':     PLAN_NONE,
    'PENDING':  PLAN_INTENT_ENTER,
    'SENT':     PLAN_ORDER_REQUESTED,
    'ACKED':    PLAN_ENTERING,
    'PARTIAL':  PLAN_ENTERING,
    'FILLED':   PLAN_OPEN,
    'CANCELED':  PLAN_NONE,
    'REJECTED':  PLAN_NONE,
    'TIMEOUT':   PLAN_NONE,
}

ORDER_STATE_TO_PLAN_EXIT = {
    'NONE':     PLAN_OPEN,
    'PENDING':  PLAN_INTENT_EXIT,
    'SENT':     PLAN_EXITING,
    'ACKED':    PLAN_EXITING,
    'PARTIAL':  PLAN_EXITING,
    'FILLED':   PLAN_NONE,
    'CANCELED':  PLAN_OPEN,
    'REJECTED':  PLAN_OPEN,
    'TIMEOUT':   PLAN_OPEN,
}

# ── EQ action_type → entry vs exit classification ──
ENTRY_ACTIONS = frozenset({'OPEN', 'ADD', 'REVERSE_OPEN'})
EXIT_ACTIONS = frozenset({'CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE',
                          'STOP_LOSS', 'EMERGENCY_CLOSE', 'SCHEDULED_CLOSE'})


def map_db_to_plan(side, stage, order_state=None, plan_state_col=None):
    """Map DB fields to a PLAN.* state.

    If plan_state_col is already set in DB, return it directly.
    Otherwise infer from side/stage/order_state.

    Args:
        side: position_state.side (e.g. 'long', 'short', None)
        stage: position_state.stage (int)
        order_state: position_state.order_state (e.g. 'NONE', 'SENT', 'FILLED')
        plan_state_col: position_state.plan_state if column exists
    Returns:
        PLAN.* state string
    """
    # If DB already has plan_state, trust it
    if plan_state_col and plan_state_col in PLAN_STATES:
        return plan_state_col

    if not side:
        return PLAN_NONE

    os_upper = (order_state or 'NONE').upper()
    stage = int(stage or 0)

    # If FILLED with stage > 0, position is open
    if os_upper == 'FILLED' and stage > 0:
        return PLAN_OPEN

    # Entry path: no existing position (stage == 0) or order_state indicates entry
    if stage == 0:
        return ORDER_STATE_TO_PLAN_ENTRY.get(os_upper, PLAN_INTENT_ENTER)

    # Position exists (stage > 0): default to OPEN unless order_state says otherwise
    if os_upper in ('NONE', 'FILLED'):
        return PLAN_OPEN
    if os_upper in ('SENT', 'ACKED', 'PARTIAL'):
        # Could be ADD or EXIT — without more context, assume entry-side
        return PLAN_OPEN
    if os_upper in ('PENDING',):
        return PLAN_OPEN

    return ORDER_STATE_TO_PLAN_ENTRY.get(os_upper, PLAN_NONE)


def map_legacy(side, stage, order_state=None):
    """Legacy string mapping (backward compatible with old _map_strategy_state).

    Returns old-style strings: FLAT, INTENT_ENTER, IN_POSITION, etc.
    """
    if not side:
        return 'FLAT'
    if order_state is not None and order_state != '':
        os_upper = (order_state or 'NONE').upper()
        mapping = {
            'NONE': 'FLAT',
            'PENDING': 'INTENT_ENTER',
            'SENT': 'ORDER_SENT',
            'ACKED': 'ORDER_ACKED',
            'PARTIAL': 'PARTIAL_FILLED',
            'FILLED': 'IN_POSITION',
            'CANCELED': 'CANCELED',
            'REJECTED': 'REJECTED',
            'TIMEOUT': 'WAIT_EXCHANGE_SYNC',
        }
        mapped = mapping.get(os_upper)
        if mapped:
            if os_upper == 'FILLED' and int(stage or 0) > 0:
                return 'IN_POSITION'
            return mapped
    stage = int(stage or 0)
    if stage == 0:
        return 'INTENT_ENTER'
    return 'IN_POSITION'


def validate_transition(from_state, to_state):
    """Check if transition from_state → to_state is valid.

    Returns:
        (ok: bool, error: str or None)
    """
    if from_state not in PLAN_STATES:
        return (False, f'invalid from_state: {from_state}')
    if to_state not in PLAN_STATES:
        return (False, f'invalid to_state: {to_state}')
    valid = TRANSITIONS.get(from_state, set())
    if to_state in valid:
        return (True, None)
    return (False, f'invalid transition: {from_state} → {to_state}')


def transition_for_action(action_type, phase='request'):
    """Get the appropriate plan_state for an EQ action.

    Args:
        action_type: e.g. 'OPEN', 'ADD', 'CLOSE', 'REDUCE'
        phase: 'request' (EQ created), 'sent' (order sent), 'filled', 'failed'
    Returns:
        PLAN.* state string
    """
    is_entry = action_type.upper() in ENTRY_ACTIONS
    is_exit = action_type.upper() in EXIT_ACTIONS

    if not is_entry and not is_exit:
        return PLAN_NONE  # unknown action, fail-safe

    if phase == 'request':
        return PLAN_ORDER_REQUESTED if is_entry else PLAN_INTENT_EXIT
    if phase == 'sent':
        return PLAN_ENTERING if is_entry else PLAN_EXITING
    if phase == 'filled':
        return PLAN_OPEN if is_entry else PLAN_NONE
    if phase == 'failed':
        # On failure, revert to previous stable state
        return PLAN_NONE if is_entry else PLAN_OPEN

    return PLAN_NONE


def is_intent_state(state):
    """True if state is an intent/pending state (not yet confirmed by exchange)."""
    return state in (PLAN_INTENT_ENTER, PLAN_ORDER_REQUESTED, PLAN_ENTERING,
                     PLAN_INTENT_EXIT, PLAN_EXITING)


def is_open_state(state):
    """True if plan considers position open."""
    return state == PLAN_OPEN
