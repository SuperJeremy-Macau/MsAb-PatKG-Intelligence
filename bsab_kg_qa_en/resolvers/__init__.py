# bsab_kg_qa_en/resolvers/__init__.py
# Compatibility exports only. New NER optimization lives under bsab_kg_qa_en/ner.
from .target_pair_resolver import TargetPairResolver, TargetPairResolution

__all__ = ["TargetPairResolver", "TargetPairResolution"]
