from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple

class CleaningRule(ABC):
    """
    Abstract base class for all data cleaning rules.
    """
    @property
    @abstractmethod
    def rule_code(self) -> str:
        """The unique identifier for the cleaning rule."""
        pass
        
    @abstractmethod
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Applies the rule to a record.
        Returns:
            Tuple[cleaned_record, list_of_corrections]
            Where each correction is a dict containing:
                {
                    "field": str,
                    "original_value": Any,
                    "corrected_value": Any,
                    "rule_code": str
                }
        """
        pass
