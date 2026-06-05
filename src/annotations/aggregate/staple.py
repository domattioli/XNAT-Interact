"""
annotations.aggregate.staple — STAPLE algorithm extension stub.

DEFERRED: This module is an intentional stub.  The STAPLE algorithm is NOT
implemented here.  It documents exactly where to plug it in.

What STAPLE Is
--------------
STAPLE = Simultaneous Truth And Performance Level Estimation (Warfield et al.,
IEEE TMI 2004).  It models each annotator's *sensitivity* and *specificity*
separately and iterates an EM (Expectation-Maximisation) algorithm to jointly
estimate:

  - The probabilistic "hidden true" segmentation W (E-step).
  - Per-annotator quality parameters p_j (sensitivity) and q_j (specificity)
    (M-step).

Convergence criterion: max change in W across iterations < epsilon (typically
1e-5).  Output is a probabilistic mask thresholded at 0.5 (or a full float
map if the caller wants the probability output).

Why It Is Deferred
------------------
- Requires scipy or a dedicated EM loop — adds a dependency not present in
  the current environment.
- The reference aggregator is sufficient to validate the seam (T006-T009).
- STAPLE touches voxel intensities and annotator-identity metadata; it needs
  more careful PHI-handling review before production use.

How To Plug It In (zero core edits)
------------------------------------
1. Implement the EM loop below (or wrap SimpleITK's STAPLE filter).
2. Uncomment the registration line at the bottom of this module.
3. Import this module anywhere before calling aggregate_set():

   from src.annotations.aggregate import staple  # triggers registration

OR call explicitly:

   from src.annotations.aggregate import register_aggregator
   from src.annotations.aggregate.staple import StapleAggregator
   register_aggregator('staple', StapleAggregator())

That is the ONLY step needed.  No edits to model.py / registry.py /
codecs.py / validate.py / aggregate/__init__.py.
"""
from __future__ import annotations

from typing import Any, List

from src.annotations.model import ConsensusResult
from src.annotations.aggregate import Aggregator

_METHOD_NAME = "staple"


class StapleAggregator(Aggregator):
    """
    STAPLE aggregator stub.

    Raises NotImplementedError until the EM loop is implemented.
    See module docstring for the full implementation guide.
    """

    def aggregate(self, payloads: List[Any], **ctx) -> ConsensusResult:
        """
        NOT IMPLEMENTED.

        Replace this body with the STAPLE EM algorithm:

          E-step : W_ij = p_j^D_ij * (1-q_j)^(1-D_ij) * prior /
                          (same + (1-p_j)^D_ij * q_j^(1-D_ij) * (1-prior))
          M-step : p_j = sum_i(W_i * D_ij) / sum_i(W_i)
                   q_j = sum_i((1-W_i) * (1-D_ij)) / sum_i(1-W_i)
          Repeat until max|W_new - W_old| < epsilon.

        Registration (after implementation):
            register_aggregator('staple', StapleAggregator())
        """
        raise NotImplementedError(
            "STAPLE aggregator is not yet implemented.  "
            "See src/annotations/aggregate/staple.py for the full "
            "implementation guide and registration instructions.  "
            "Use 'reference' aggregator for development/testing."
        )


# ---------------------------------------------------------------------------
# NOT auto-registered.  Explicit opt-in only (see module docstring).
# ---------------------------------------------------------------------------
# To register once implemented:
#   register_aggregator('staple', StapleAggregator())
